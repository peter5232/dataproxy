package org.secretflow.dataproxy.plugin.hive.producer;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.secretflow.dataproxy.plugin.hive.writer.HiveRecordWriter;
import org.secretflow.dataproxy.plugin.hive.config.HiveCommandConfig;
import org.secretflow.dataproxy.plugin.hive.config.HiveWriteConfig;
import org.secretflow.dataproxy.plugin.hive.config.TaskConfig;
import org.secretflow.dataproxy.plugin.hive.converter.HiveParamConverter;
import org.secretflow.dataproxy.plugin.hive.reader.HiveDoGetContext;
import org.secretflow.dataproxy.plugin.hive.reader.HiveReader;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.utils.GrpcUtils;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.core.service.TicketService;
import org.secretflow.dataproxy.core.service.impl.CacheTicketService;
import org.secretflow.dataproxy.core.spi.producer.DataProxyFlightProducer;
import org.secretflow.dataproxy.core.writer.Writer;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

@Slf4j
public class HiveFlightProducer extends NoOpFlightProducer implements DataProxyFlightProducer {
    private final TicketService ticketService = CacheTicketService.getInstance();

    @Override
    public String getProducerName() {
        return "hive";
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        final Any any = GrpcUtils.parseOrThrow(descriptor.getCommand());
        try {
            boolean isPut = false;

            HiveCommandConfig<?> commandConfig = switch (any.getTypeUrl()) {
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshSqlQuery" ->
                        new HiveParamConverter().convert(any.unpack(Flightinner.CommandDataMeshSqlQuery.class));
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshQuery" ->
                        new HiveParamConverter().convert(any.unpack(Flightinner.CommandDataMeshQuery.class));
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshUpdate" -> {
                    isPut = true;
                    yield new HiveParamConverter().convert(any.unpack(Flightinner.CommandDataMeshUpdate.class));
                }
                default -> throw CallStatus.INVALID_ARGUMENT
                        .withDescription("Unknown command type")
                        .toRuntimeException();
            };

            log.info("HiveFlightProducer#getFlightInfo, commandConfig: {}", JsonUtils.toString(commandConfig));

            byte[] bytes;

            List<FlightEndpoint> endpointList;
            if (isPut) {
                bytes = ticketService.generateTicket(ParamWrapper.of(getProducerName(), commandConfig));
                Flightdm.TicketDomainDataQuery ticketDomainDataQuery = Flightdm.TicketDomainDataQuery.newBuilder().setDomaindataHandle(new String(bytes)).build();
                bytes = Any.pack(ticketDomainDataQuery).toByteArray();
                endpointList = Collections.singletonList(
                        new FlightEndpoint(new Ticket(bytes), FlightServerContext.getInstance().getFlightServerConfig().getLocation())
                );
            } else {
                bytes = ticketService.generateTicket(ParamWrapper.of(getProducerName(), commandConfig));
                endpointList = Collections.singletonList(
                        new FlightEndpoint(new Ticket(bytes), FlightServerContext.getInstance().getFlightServerConfig().getLocation())
                );
            }
            // Only the protocol is used, and the concrete schema is not returned here.
            return new FlightInfo(DataProxyFlightProducer.DEFACT_SCHEMA, descriptor, endpointList, 0, 0,true, IpcOption.DEFAULT);
        } catch (InvalidProtocolBufferException e) {
            throw CallStatus.INVALID_ARGUMENT
                    .withCause(e)
                    .withDescription(e.getMessage())
                    .toRuntimeException();
        } catch (Exception e) {
            log.error("getFlightInfo error", e);
            throw CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException();
        }
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        ParamWrapper paramWrapper = ticketService.getParamWrapper(ticket.getBytes());
        ArrowReader hiveReader = null;

        try {
            Object param = paramWrapper.param();

            if (param == null) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The hive read parameter is null");
            }

            if (param instanceof HiveCommandConfig<?> hiveCommandConfig) {
                HiveDoGetContext hiveDoGetContext = new HiveDoGetContext(hiveCommandConfig);

                List<TaskConfig> taskConfigs = hiveDoGetContext.getTaskConfigs();
                hiveReader = new HiveReader(new RootAllocator(), taskConfigs.get(0));
            } else {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The hive read parameter is invalid, type url: " + param.getClass());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            listener.start(hiveReader.getVectorSchemaRoot());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        while (true) {
            if (context.isCancelled()) {
                log.warn("reader is cancelled");
                break;
            }

            try {
                if (hiveReader.loadNextBatch()) {
                    listener.putNext();
                } else{
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        listener.completed();
        log.info("end");
    }

    public Runnable acceptPut(
            CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream
    ) {
        final Any any = GrpcUtils.parseOrThrow(flightStream.getDescriptor().getCommand());

        if(!"type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.TicketDomainDataQuery".equals(any.getTypeUrl())) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The hive write parameter is invalid, type url: " + any.getTypeUrl());
        }

        return () -> {
            try {
                Flightdm.TicketDomainDataQuery unpack = any.unpack(Flightdm.TicketDomainDataQuery.class);
                HiveWriteConfig writeConfig = ticketService.getParamWrapper(unpack.getDomaindataHandle().getBytes()).unwrap(HiveWriteConfig.class);

                Writer writer;
                int count = 0;
                writer = new HiveRecordWriter(writeConfig);
                String askMsg;
                VectorSchemaRoot vectorSchemaRoot;
                while (flightStream.next()) {
                    vectorSchemaRoot = flightStream.getRoot();
                    int rowCount = vectorSchemaRoot.getRowCount();
                    askMsg = "row count: " + rowCount;
                    writer.write(vectorSchemaRoot);

                    try (BufferAllocator ba = new RootAllocator(1024);
                         final ArrowBuf buffer = ba.buffer(askMsg.getBytes(StandardCharsets.UTF_8).length)) {
                        ackStream.onNext(PutResult.metadata(buffer));
                    }
                    count += rowCount;
                }
                writer.flush();
                ackStream.onCompleted();
                log.info("put data over! all count: {}", count);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
}

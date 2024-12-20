// Copyright 2024 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "pybind11/pybind11.h"

#include "dataproxy_sdk/cc/api.h"
#include "dataproxy_sdk/cc/exception.h"

namespace py = pybind11;

namespace dataproxy_sdk {

void DeletePtr(void* ptr) {
  if (ptr) {
    free(ptr);
  }
}

PYBIND11_MODULE(_lib, m) {
  m.doc() = R"pbdoc(
              Secretflow-DataProxy-SDK Python Library
                  )pbdoc";

  py::register_exception_translator(
      [](std::exception_ptr p) {  // NOLINT: pybind11
        try {
          if (p) {
            std::rethrow_exception(p);
          }
        } catch (const yacl::Exception& e) {
          // Translate this exception to a standard RuntimeError
          PyErr_SetString(PyExc_RuntimeError,
                          fmt::format("what: \n\t{}\n", e.what()).c_str());
        }
      });

  py::class_<DataProxyFile, std::unique_ptr<DataProxyFile>>(m, "DataProxyFile")
      .def(py::init(
          [](const py::bytes& config_str) -> std::unique_ptr<DataProxyFile> {
            proto::DataProxyConfig config;
            config.ParseFromString(config_str);
            return DataProxyFile::Make(config);
          }))
      .def("download_file",
           [](DataProxyFile& self, const py::bytes& info_str,
              const std::string& file_path, int file_format) {
             proto::DownloadInfo info;
             info.ParseFromString(info_str);

             self.DownloadFile(info, file_path,
                               static_cast<proto::FileFormat>(file_format));
           })
      .def("upload_file",
           [](DataProxyFile& self, const py::bytes& info_str,
              const std::string& file_path, int file_format) {
             proto::UploadInfo info;
             info.ParseFromString(info_str);

             self.UploadFile(info, file_path,
                             static_cast<proto::FileFormat>(file_format));
           })
      .def("close", &DataProxyFile::Close);

  py::class_<DataProxyStreamReader, std::unique_ptr<DataProxyStreamReader>>(
      m, "DataProxyStreamReader")
      .def("get",
           [](DataProxyStreamReader& self) -> py::object {
             std::shared_ptr<arrow::RecordBatch> batch;
             self.Get(&batch);
             if (batch == nullptr) {
               return py::none();
             }
             ArrowArray* ret_array = (ArrowArray*)malloc(sizeof(ArrowArray));
             CHECK_ARROW_OR_THROW(arrow::ExportRecordBatch(*batch, ret_array));
             return py::capsule(ret_array, nullptr, DeletePtr);
           })
      .def("schema", [](DataProxyStreamReader& self) -> py::object {
        auto schema = self.Schema();
        DATAPROXY_ENFORCE(schema != nullptr);
        ArrowSchema* ret_schema = (ArrowSchema*)malloc(sizeof(*ret_schema));
        CHECK_ARROW_OR_THROW(arrow::ExportSchema(*schema, ret_schema));
        return py::capsule(ret_schema, "arrow_schema", DeletePtr);
      });

  py::class_<DataProxyStreamWriter, std::unique_ptr<DataProxyStreamWriter>>(
      m, "DataProxyStreamWriter")
      .def("put",
           [](DataProxyStreamWriter& self, py::capsule schema_capsule,
              py::capsule array_capsule) {
             ArrowArray* array = array_capsule.get_pointer<ArrowArray>();
             ArrowSchema* schema = schema_capsule.get_pointer<ArrowSchema>();
             ASSIGN_DP_OR_THROW(auto batch,
                                arrow::ImportRecordBatch(array, schema));
             self.Put(batch);
           })
      .def("close", [](DataProxyStreamWriter& self) { self.Close(); });

  py::class_<DataProxyStream, std::shared_ptr<DataProxyStream>>(
      m, "DataProxyStream")
      .def(py::init(
          [](const py::bytes& config_str) -> std::shared_ptr<DataProxyStream> {
            proto::DataProxyConfig config;
            config.ParseFromString(config_str);
            return DataProxyStream::Make(config);
          }))
      .def("get_reader",
           [](DataProxyStream& self, const py::bytes& info_str)
               -> std::unique_ptr<DataProxyStreamReader> {
             proto::DownloadInfo info;
             info.ParseFromString(info_str);
             return self.GetReader(info);
           })
      .def("get_writer",
           [](DataProxyStream& self, const py::bytes& info_str)
               -> std::unique_ptr<DataProxyStreamWriter> {
             proto::UploadInfo info;
             info.ParseFromString(info_str);
             return self.GetWriter(info);
           });
}

}  // namespace dataproxy_sdk
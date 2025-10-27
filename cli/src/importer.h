/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <filesystem>
#include <iostream>

#include "arrow/api.h"
#include "arrow/table.h"
#include "graphar/api/arrow_writer.h"
#include "graphar/api/high_level_writer.h"
#include "graphar/convert_to_arrow_type.h"
#include "graphar/graph_info.h"
#include "graphar/high-level/graph_reader.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "util.h"

#define PYBIND11_DETAILED_ERROR_MESSAGES

namespace py = pybind11;
namespace fs = std::filesystem;

struct GraphArConfig {
  std::string path;
  std::string name;
  std::string version;
};

struct Property {
  std::string name;
  std::string data_type;
  bool is_primary;
  bool nullable;
  std::string remap_to;
};

struct PropertyGroup {
  std::string file_type;
  std::vector<Property> properties;
};

struct Source {
  std::string file_type;
  std::vector<std::string> path;
  char delimiter;
  std::unordered_map<std::string, std::string> columns;
};

struct Vertex {
  std::string type;
  std::vector<std::string> labels;
  int chunk_size;
  std::string validate_level;
  std::string prefix;
  std::vector<PropertyGroup> property_groups;
  std::vector<Source> sources;
};

struct AdjList {
  bool ordered;
  std::string aligned_by;
  std::string file_type;
};

struct Edge {
  std::string edge_type;
  std::string src_type;
  std::string src_prop;
  std::string src_edge_prop;
  std::string dst_type;
  std::string dst_prop;
  std::string dst_edge_prop;
  int chunk_size;
  std::string validate_level;
  std::string prefix;
  std::vector<AdjList> adj_lists;
  std::vector<PropertyGroup> property_groups;
  std::vector<Source> sources;
};

struct ImportSchema {
  std::vector<Vertex> vertices;
  std::vector<Edge> edges;
};

struct ImportConfig {
  GraphArConfig graphar_config;
  ImportSchema import_schema;
};

ImportConfig ConvertPyDictToConfig(const py::dict& config_dict) {
  ImportConfig import_config;

  std::cout << "Starting conversion of Python dict to ImportConfig..." << std::endl;

  auto graphar_dict = config_dict["graphar"].cast<py::dict>();
  std::cout << "Extracting GraphAr configuration..." << std::endl;
  import_config.graphar_config.path = graphar_dict["path"].cast<std::string>();
  import_config.graphar_config.name = graphar_dict["name"].cast<std::string>();
  import_config.graphar_config.version =
      graphar_dict["version"].cast<std::string>();
  std::cout << "GraphAr config extracted: path='" << import_config.graphar_config.path
            << "', name='" << import_config.graphar_config.name
            << "', version='" << import_config.graphar_config.version << "'" << std::endl;

  auto schema_dict = config_dict["import_schema"].cast<py::dict>();
  std::cout << "Extracting import schema..." << std::endl;

  auto vertices_list = schema_dict["vertices"].cast<std::vector<py::dict>>();
  std::cout << "Found " << vertices_list.size() << " vertex entries. Processing..." << std::endl;
  for (const auto& vertex_dict : vertices_list) {
    Vertex vertex;
    vertex.type = vertex_dict["type"].cast<std::string>();
    std::cout << "\tProcessing vertex type '" << vertex.type << "'..." << std::endl;
    vertex.chunk_size = vertex_dict["chunk_size"].cast<int>();
    vertex.prefix = vertex_dict["prefix"].cast<std::string>();
    vertex.validate_level = vertex_dict["validate_level"].cast<std::string>();
    vertex.labels = vertex_dict["labels"].cast<std::vector<std::string>>();
    std::cout << "\t\tChunk size: " << vertex.chunk_size
              << ", Prefix: '" << vertex.prefix
              << "', Validate level: '" << vertex.validate_level << "'" << std::endl;
    if (!vertex.labels.empty()) {
      std::cout << "\t\tLabels: ";
      for (const auto& label : vertex.labels) {
        std::cout << label << " ";
      }
      std::cout << std::endl;
    }

    auto pg_list = vertex_dict["property_groups"].cast<std::vector<py::dict>>();
    std::cout << "\t\tFound " << pg_list.size() << " property group(s)." << std::endl;
    for (const auto& pg_dict : pg_list) {
      PropertyGroup pg;
      pg.file_type = pg_dict["file_type"].cast<std::string>();
      std::cout << "\t\t\tProcessing property group with file type '" << pg.file_type << "'..." << std::endl;

      auto prop_list = pg_dict["properties"].cast<std::vector<py::dict>>();
      std::cout << "\t\t\tFound " << prop_list.size() << " property(ies)." << std::endl;
      for (const auto& prop_dict : prop_list) {
        Property prop;
        prop.name = prop_dict["name"].cast<std::string>();
        prop.data_type = prop_dict["data_type"].cast<std::string>();
        prop.is_primary = prop_dict["is_primary"].cast<bool>();
        prop.nullable = prop_dict["nullable"].cast<bool>();
        std::cout << "\t\t\t\tProperty: name='" << prop.name
                  << "', type='" << prop.data_type
                  << "', primary='" << (prop.is_primary ? "yes" : "no")
                  << "', nullable='" << (prop.nullable ? "yes" : "no") << "'" << std::endl;
        if (prop_dict.contains("remap_to") && !prop_dict["remap_to"].is_none()) {
          prop.remap_to = prop_dict["remap_to"].cast<std::string>();
          std::cout << "\t\t\t\t\tRemap target: '" << prop.remap_to << "'" << std::endl;
        }
        pg.properties.emplace_back(prop);
      }
      vertex.property_groups.emplace_back(pg);
    }

    auto source_list = vertex_dict["sources"].cast<std::vector<py::dict>>();
    std::cout << "\t\tFound " << source_list.size() << " source(s)." << std::endl;
    for (const auto& source_dict : source_list) {
      Source src;
      src.file_type = source_dict["file_type"].cast<std::string>();
      src.path = source_dict["path"].cast<std::vector<std::string>>();
      src.delimiter = source_dict["delimiter"].cast<char>();
      src.columns = source_dict["columns"]
                        .cast<std::unordered_map<std::string, std::string>>();

      std::cout << "\t\t\tSource: file_type='" << src.file_type
                << "', delimiter='" << src.delimiter << "'" << std::endl;
      std::cout << "\t\t\t\tPaths: ";
      for (const auto& p : src.path) {
        std::cout << p << "; ";
      }
      std::cout << std::endl;
      std::cout << "\t\t\t\tColumn mapping: ";
      for (const auto& [key, value] : src.columns) {
        std::cout << key << "->" << value << "; ";
      }
      std::cout << std::endl;

      vertex.sources.emplace_back(src);
    }

    import_config.import_schema.vertices.emplace_back(vertex);
    std::cout << "\tFinished processing vertex type '" << vertex.type << "'." << std::endl;
  }
  std::cout << "Completed processing all vertices." << std::endl;

  auto edges_list = schema_dict["edges"].cast<std::vector<py::dict>>();
  std::cout << "Found " << edges_list.size() << " edge entries. Processing..." << std::endl;
  for (const auto& edge_dict : edges_list) {
    Edge edge;
    edge.edge_type = edge_dict["edge_type"].cast<std::string>();
    edge.src_type = edge_dict["src_type"].cast<std::string>();
    edge.src_prop = edge_dict["src_prop"].cast<std::string>();
    edge.src_edge_prop = edge_dict["src_edge_prop"].cast<std::string>();
    edge.dst_type = edge_dict["dst_type"].cast<std::string>();
    edge.dst_prop = edge_dict["dst_prop"].cast<std::string>();
    edge.dst_edge_prop = edge_dict["dst_edge_prop"].cast<std::string>();
    edge.chunk_size = edge_dict["chunk_size"].cast<int>();
    edge.validate_level = edge_dict["validate_level"].cast<std::string>();
    edge.prefix = edge_dict["prefix"].cast<std::string>();
    std::cout << "\tProcessing edge: '" << edge.src_type << "' --(" << edge.edge_type
              << ")-> '" << edge.dst_type << "'" << std::endl;
    std::cout << "\t\tChunk size: " << edge.chunk_size
              << ", Prefix: '" << edge.prefix
              << "', Validate level: '" << edge.validate_level << "'" << std::endl;
    std::cout << "\t\tSource property mapping: '" << edge.src_prop << "' -> '" << edge.src_edge_prop
              << "', Destination: '" << edge.dst_prop << "' -> '" << edge.dst_edge_prop << "'" << std::endl;

    auto adj_list_dicts = edge_dict["adj_lists"].cast<std::vector<py::dict>>();
    std::cout << "\t\tFound " << adj_list_dicts.size() << " adjacency list(s)." << std::endl;
    for (const auto& adj_list_dict : adj_list_dicts) {
      AdjList adj_list;
      adj_list.ordered = adj_list_dict["ordered"].cast<bool>();
      adj_list.aligned_by = adj_list_dict["aligned_by"].cast<std::string>();
      adj_list.file_type = adj_list_dict["file_type"].cast<std::string>();
      std::cout << "\t\t\tAdjacency list: ordered='" << (adj_list.ordered ? "true" : "false")
                << "', aligned_by='" << adj_list.aligned_by
                << "', file_type='" << adj_list.file_type << "'" << std::endl;
      edge.adj_lists.emplace_back(adj_list);
    }

    auto edge_pg_list =
        edge_dict["property_groups"].cast<std::vector<py::dict>>();
    std::cout << "\t\tFound " << edge_pg_list.size() << " edge property group(s)." << std::endl;
    for (const auto& edge_pg_dict : edge_pg_list) {
      PropertyGroup edge_pg;
      edge_pg.file_type = edge_pg_dict["file_type"].cast<std::string>();
      auto edge_prop_list =
          edge_pg_dict["properties"].cast<std::vector<py::dict>>();

      std::cout << "\t\t\tProperty group with file type '" << edge_pg.file_type
                << "' has " << edge_prop_list.size() << " property(ies)." << std::endl;
      for (const auto& prop_dict : edge_prop_list) {
        Property edge_prop;
        edge_prop.name = prop_dict["name"].cast<std::string>();
        edge_prop.data_type = prop_dict["data_type"].cast<std::string>();
        edge_prop.is_primary = prop_dict["is_primary"].cast<bool>();
        edge_prop.nullable = prop_dict["nullable"].cast<bool>();
        std::cout << "\t\t\t\tProperty: name='" << edge_prop.name
                  << "', type='" << edge_prop.data_type
                  << "', primary='" << (edge_prop.is_primary ? "yes" : "no")
                  << "', nullable='" << (edge_prop.nullable ? "yes" : "no") << "'" << std::endl;
        edge_pg.properties.emplace_back(edge_prop);
      }
      edge.property_groups.emplace_back(edge_pg);
    }

    auto edge_source_list = edge_dict["sources"].cast<std::vector<py::dict>>();
    std::cout << "\t\tFound " << edge_source_list.size() << " edge source(s)." << std::endl;
    for (const auto& edge_source_dict : edge_source_list) {
      Source edge_src;
      edge_src.file_type = edge_source_dict["file_type"].cast<std::string>();
      edge_src.path = edge_source_dict["path"].cast<std::vector<std::string>>();
      edge_src.delimiter = edge_source_dict["delimiter"].cast<char>();
      edge_src.columns =
          edge_source_dict["columns"]
              .cast<std::unordered_map<std::string, std::string>>();

      std::cout << "\t\t\tEdge source: file_type='" << edge_src.file_type
                << "', delimiter='" << edge_src.delimiter << "'" << std::endl;
      std::cout << "\t\t\t\tPaths: ";
      for (const auto& p : edge_src.path) {
        std::cout << p << "; ";
      }
      std::cout << std::endl;
      std::cout << "\t\t\t\tColumn mapping: ";
      for (const auto& [key, value] : edge_src.columns) {
        std::cout << key << "->" << value << "; ";
      }
      std::cout << std::endl;

      edge.sources.emplace_back(edge_src);
    }

    import_config.import_schema.edges.emplace_back(edge);
    std::cout << "\tFinished processing edge '" << edge.src_type << "' --(" << edge.edge_type
              << ")-> '" << edge.dst_type << "'." << std::endl;
  }
  std::cout << "Completed processing all edges." << std::endl;

  std::cout << "Successfully converted Python dict to ImportConfig." << std::endl;
  return import_config;
}

std::string DoImport(const py::dict& config_dict) {
  std::cout << "Starting DoImport function..." << std::endl;
  auto import_config = ConvertPyDictToConfig(config_dict);
  std::cout << "Configuration converted successfully." << std::endl;

  auto version =
      graphar::InfoVersion::Parse(import_config.graphar_config.version).value();
  fs::path save_path = import_config.graphar_config.path;

  std::unordered_map<std::string, graphar::IdType> vertex_chunk_sizes;
  std::unordered_map<std::string, int64_t> vertex_counts;

  std::map<std::pair<std::string, std::string>,
           std::unordered_map<std::shared_ptr<arrow::Scalar>, graphar::IdType,
                              arrow::Scalar::Hash, arrow::Scalar::PtrsEqual>>
      vertex_prop_index_map;

  std::unordered_map<std::string, std::vector<std::string>>
      vertex_props_for_remapping;
  std::map<std::pair<std::string, std::string>, graphar::Property>
      vertex_prop_property_map;
  for (const auto& edge : import_config.import_schema.edges) {
    vertex_props_for_remapping[edge.src_type].emplace_back(edge.src_prop);
    vertex_props_for_remapping[edge.dst_type].emplace_back(edge.dst_prop);
  }

  graphar::VertexInfoVector vertices_info;
  std::vector<std::string> vertices_labels;
  std::cout << "Processing " << import_config.import_schema.vertices.size()
            << " vertices..." << std::endl;
  std::unordered_map<std::string, std::string> primary_keys;
  std::unordered_map<std::string, std::vector<std::shared_ptr<graphar::PropertyGroup>>> pgs_by_vtype;
  for (const auto& vertex : import_config.import_schema.vertices) {
    std::cout << "Processing vertex: " << vertex.type << std::endl;
    vertex_chunk_sizes[vertex.type] = vertex.chunk_size;

    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();
    std::string primary_key;
    std::cout << "  Processing " << vertex.property_groups.size()
              << " property groups for vertex " << vertex.type << std::endl;
    for (const auto& pg : vertex.property_groups) {
      std::cout << "    Processing property group with file type: "
                << pg.file_type << std::endl;
      std::vector<graphar::Property> props;
      for (const auto& prop : pg.properties) {
        if (prop.is_primary) {
          if (!primary_key.empty()) {
            throw std::runtime_error("Multiple primary keys found in vertex " +
                                     vertex.type);
          }
          primary_key = prop.name;
        }
        graphar::Property property(
            prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
            prop.is_primary, prop.nullable);
        props.emplace_back(property);
        vertex_prop_property_map[std::make_pair(vertex.type, prop.name)] =
            property;
      }
      // TODO: add prefix parameter in config
      auto property_group = graphar::CreatePropertyGroup(
          props, graphar::StringToFileType(pg.file_type));
      pgs.emplace_back(property_group);
    }
    pgs_by_vtype[vertex.type] = pgs;
    if (!primary_key.empty()) {
      primary_keys[vertex.type] = primary_key;
    }
  }
  std::map<std::pair<std::string, std::string>, std::pair<std::string, std::string>> for_custom_remapping;
  for (const auto& vertex : import_config.import_schema.vertices) {
    for (const auto& pg : vertex.property_groups) {
      for (const auto& prop : pg.properties) {
        if (!prop.remap_to.empty()) {
          for_custom_remapping[std::make_pair(vertex.type, prop.name)] = std::make_pair(prop.remap_to, primary_keys[prop.remap_to]);
          vertex_props_for_remapping[prop.remap_to].emplace_back(
              primary_keys[prop.remap_to]);
        }
      }
    }
  }
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> vertex_tables_with_index;
  std::unordered_map<std::string, std::shared_ptr<graphar::VertexPropertyWriter>> vertex_prop_writers;
  for (const auto& vertex : import_config.import_schema.vertices) {
    auto vertex_info =
        graphar::CreateVertexInfo(vertex.type, vertex.chunk_size, pgs_by_vtype[vertex.type],
                                  vertex.labels, vertex.prefix, version);
    vertices_info.push_back(vertex_info);
    auto file_name = vertex.type + ".vertex.yaml";
    vertex_info->Save(save_path / file_name);
    auto save_path_str = save_path.string();
    save_path_str += "/";
    auto vertex_prop_writer = graphar::VertexPropertyWriter::Make(
                                  vertex_info, save_path_str,
                                  StringToValidateLevel(vertex.validate_level))
                                  .value();

    std::vector<std::shared_ptr<arrow::Table>> vertex_tables;
    for (const auto& source : vertex.sources) {
      std::vector<std::string> column_names;
      for (const auto& [key, value] : source.columns) {
        column_names.emplace_back(key);
      }

      std::shared_ptr<arrow::Table> table;
      {
        std::vector<std::shared_ptr<arrow::Table>> file_tables(
            source.path.size());
        for (int i = 0; i < source.path.size(); ++i) {
          file_tables[i] = GetDataFromFile(source.path[i], column_names,
                                           source.delimiter, source.file_type);
        }
        table = ConcatenateTables(file_tables).ValueOrDie();
      }

      std::unordered_map<std::string, Property> column_prop_map;
      std::unordered_map<std::string, std::string> reversed_columns_config;
      for (const auto& [key, value] : source.columns) {
        reversed_columns_config[value] = key;
      }
      for (const auto& pg : vertex.property_groups) {
        for (const auto& prop : pg.properties) {
          column_prop_map[reversed_columns_config[prop.name]] = prop;
        }
      }
      std::unordered_map<
          std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>
          columns_to_change;
      for (const auto& [column, prop] : column_prop_map) {
        auto arrow_data_type = graphar::DataType::DataTypeToArrowDataType(
            graphar::DataType::TypeNameToDataType(prop.data_type));
        auto arrow_column = table->GetColumnByName(column);
        // TODO: whether need to check duplicate values for primary key?
        if (!prop.nullable) {
          for (const auto& chunk : arrow_column->chunks()) {
            if (chunk->null_count() > 0) {
              throw std::runtime_error("Non-nullable column '" + column +
                                       "' has null values");
            }
          }
        }
        // TODO: check this
        if (column != prop.name ||
            arrow_column->type()->id() != arrow_data_type->id()) {
          columns_to_change[column] =
              std::make_pair(prop.name, arrow_data_type);
        }
        if (!prop.remap_to.empty()) {}
      }
      table = ChangeNameAndDataType(table, columns_to_change);

      vertex_tables.emplace_back(table);
    }
    std::shared_ptr<arrow::Table> merged_vertex_table =
        MergeTables(vertex_tables);
    // TODO: check all fields in props

    graphar::IdType start_chunk_index = 0;

    auto vertex_table_with_index =
        vertex_prop_writer
            ->AddIndexColumn(merged_vertex_table, start_chunk_index,
                             vertex_info->GetChunkSize())
            .value();
    if (vertex_props_for_remapping.find(vertex.type) !=
        vertex_props_for_remapping.end()) {
      for (const auto& vertex_prop : vertex_props_for_remapping[vertex.type]) {
        auto key = std::make_pair(vertex.type, vertex_prop);
        if (vertex_prop_index_map.find(key) != vertex_prop_index_map.end()) {
          continue;
        }
        std::cout << "Creating map for " << vertex.type << " -- " << vertex_prop << std::endl;
        vertex_prop_index_map[std::make_pair(vertex.type, vertex_prop)] =
            TableToUnorderedMap(vertex_table_with_index, vertex_prop,
                                graphar::GeneralParams::kVertexIndexCol);
      }
    }
    vertex_tables_with_index[vertex.type] = vertex_table_with_index;
    vertex_prop_writers[vertex.type] = vertex_prop_writer;
  }
  for (const auto& vertex : import_config.import_schema.vertices) {
    graphar::IdType start_chunk_index = 0;

    auto table = vertex_tables_with_index[vertex.type];
    for (auto &column_name: table->ColumnNames()) {
      auto key = std::make_pair(vertex.type, column_name);
      if (for_custom_remapping.find(key) != for_custom_remapping.end()) {
        key = for_custom_remapping[key];
        std::cout << "Remapping column: " << column_name << " of " << vertex.type << " to " << key.second << std::endl;
        auto column = table->GetColumnByName(column_name);
        if (column->type()->id() != arrow::Type::INT64) {
          throw std::runtime_error("Cannot remap column " + column_name +
                                   " becuase it is not INT64 type");
        }
        arrow::Int64Builder builder;
        for (int64_t i = 0; i < table->num_rows(); ++i) {
          auto maybe_key_scalar = column->GetScalar(i);
          if (!maybe_key_scalar.ok()) {
            throw std::runtime_error("Cannot get scalar  on index " + std::to_string(i) + " from column " +
                                     column_name + " because " + maybe_key_scalar.status().ToString());
          }
          auto key_scalar = maybe_key_scalar.ValueOrDie();
          if (!(key_scalar->is_valid)) {
              if (!builder.AppendNull().ok()) {
                throw std::runtime_error("Builder error while remapping");
              }
              continue;
          }
          if (vertex_prop_index_map[key].find(key_scalar) == vertex_prop_index_map[key].end()) {
              throw std::runtime_error("Cannot remap column " + column_name +
                                       " becuase it has value " +
                                       key_scalar->ToString() + " on index " + std::to_string(i) + " that is not in the property " + key.second);
          }
          auto value_scalar = vertex_prop_index_map[key].at(key_scalar);
          if (!builder.Append(value_scalar).ok()) {
            throw std::runtime_error("Builder error while remapping");
          }
        }
        std::shared_ptr<arrow::Array> new_array;
        if (!builder.Finish(&new_array).ok()) {
          throw std::runtime_error("Builder error while remapping");
        }
        auto new_column = std::make_shared<arrow::ChunkedArray>(new_array);
        auto new_field = arrow::field(column_name, new_column->type());
        auto index = table->schema()->GetFieldIndex(column_name);
        table = table->RemoveColumn(index).ValueOrDie();
        table = table->AddColumn(index, new_field, new_column).ValueOrDie();
      }
    }
    std::cout << "Writing vertex " << vertex.type << " to file" << std::endl;

    for (const auto& property_group : pgs_by_vtype[vertex.type]) {
      vertex_prop_writers[vertex.type]->WriteTable(table, property_group,
                                     start_chunk_index);
    }
    auto vertex_count = table->num_rows();
    vertex_counts[vertex.type] = vertex_count;
    vertex_prop_writers[vertex.type]->WriteVerticesNum(vertex_count);

    for (auto& label : vertex.labels) {
      vertices_labels.push_back(label);
    }
    std::cout << "Writing vertex " << vertex.type << " done" << std::endl;
  }

  graphar::EdgeInfoVector edges_info;
  std::cout << "Processing " << import_config.import_schema.edges.size()
            << " edges..." << std::endl;
  for (const auto& edge : import_config.import_schema.edges) {
    std::cout << "Processing edge: " << edge.src_type << " -> "
              << edge.edge_type << " -> " << edge.dst_type << std::endl;
    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();

    for (const auto& pg : edge.property_groups) {
      std::cout << "  Processing property group with file type: "
                << pg.file_type << std::endl;
      std::vector<graphar::Property> props;
      for (const auto& prop : pg.properties) {
        std::cout << "    Adding property: " << prop.name
                  << " (data type: " << prop.data_type
                  << ", primary: " << (prop.is_primary ? "yes" : "no")
                  << ", nullable: " << (prop.nullable ? "yes" : "no") << ")"
                  << std::endl;
        props.emplace_back(graphar::Property(
            prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
            prop.is_primary, prop.nullable));
      }
      // TODO: add prefix parameter in config
      auto property_group = graphar::CreatePropertyGroup(
          props, graphar::StringToFileType(pg.file_type));
      pgs.emplace_back(property_group);
    }
    graphar::AdjacentListVector adj_lists;
    for (const auto& adj_list : edge.adj_lists) {
      std::cout << "  Configuring adjacency list: ordered="
                << (adj_list.ordered ? "true" : "false")
                << ", aligned_by=" << adj_list.aligned_by
                << ", file_type=" << adj_list.file_type << std::endl;
      // TODO: add prefix parameter in config
      adj_lists.emplace_back(graphar::CreateAdjacentList(
          graphar::OrderedAlignedToAdjListType(adj_list.ordered,
                                               adj_list.aligned_by),
          graphar::StringToFileType(adj_list.file_type)));
    }

    // TODO: add directed parameter in config

    bool directed = true;
    // TODO: whether prefix has default value?

    std::cout << "Creating EdgeInfo for edge: " << edge.src_type << "-"
              << edge.edge_type << "-" << edge.dst_type
              << " with chunk_size=" << edge.chunk_size << std::endl;
    auto edge_info = graphar::CreateEdgeInfo(
        edge.src_type, edge.edge_type, edge.dst_type, edge.chunk_size,
        vertex_chunk_sizes[edge.src_type], vertex_chunk_sizes[edge.dst_type],
        directed, adj_lists, pgs, edge.prefix, version);
    edges_info.push_back(edge_info);
    auto file_name =
        ConcatEdgeTriple(edge.src_type, edge.edge_type, edge.dst_type) +
        ".edge.yaml";
    std::cout << "Saving edge info to: " << (save_path / file_name)
              << std::endl;
    edge_info->Save(save_path / file_name);
    auto save_path_str = save_path.string();
    save_path_str += "/";
    for (const auto& adj_list : adj_lists) {
      int64_t vertex_count;
      if (adj_list->GetType() == graphar::AdjListType::ordered_by_source ||
          adj_list->GetType() == graphar::AdjListType::unordered_by_source) {
        vertex_count = vertex_counts[edge.src_type];
        std::cout << "Using source vertex count: " << vertex_count
                  << " for adj_list type "
                  << static_cast<int>(adj_list->GetType()) << std::endl;
      } else {
        vertex_count = vertex_counts[edge.dst_type];
        std::cout << "Using destination vertex count: " << vertex_count
                  << " for adj_list type "
                  << static_cast<int>(adj_list->GetType()) << std::endl;
      }
      std::vector<std::shared_ptr<arrow::Table>> edge_tables;

      for (const auto& source : edge.sources) {
        std::cout << "  Processing source file(s): ";
        for (const auto& path : source.path) {
          std::cout << path << " ";
        }
        std::cout << "\n  File type: " << source.file_type
                  << ", Delimiter: " << source.delimiter << std::endl;

        std::vector<std::string> column_names;
        for (const auto& [key, value] : source.columns) {
          column_names.emplace_back(key);
        }

        std::shared_ptr<arrow::Table> table;
        {
          std::vector<std::shared_ptr<arrow::Table>> file_tables(
              source.path.size());
          for (int i = 0; i < source.path.size(); ++i) {
            std::cout << "    Loading file: " << source.path[i] << std::endl;
            file_tables[i] =
                GetDataFromFile(source.path[i], column_names, source.delimiter,
                                source.file_type);
          }
          table = ConcatenateTables(file_tables).ValueOrDie();
          std::cout << "    Loaded table with " << table->num_rows()
                    << " rows and " << table->num_columns() << " columns."
                    << std::endl;
        }

        std::unordered_map<std::string, graphar::Property> column_prop_map;
        std::unordered_map<std::string, std::string> reversed_columns;
        for (const auto& [key, value] : source.columns) {
          reversed_columns[value] = key;
        }

        for (const auto& pg : edge.property_groups) {
          for (const auto& prop : pg.properties) {
            column_prop_map[reversed_columns[prop.name]] = graphar::Property(
                prop.name,
                graphar::DataType::TypeNameToDataType(prop.data_type),
                prop.is_primary, prop.nullable);
          }
        }
        const auto& src_prop = vertex_prop_property_map.at(
            std::make_pair(edge.src_type, edge.src_prop));
        column_prop_map[reversed_columns.at(edge.src_edge_prop)] =
            graphar::Property(edge.src_edge_prop, src_prop.type,
                              src_prop.is_primary, src_prop.is_nullable);
        const auto& dst_prop = vertex_prop_property_map.at(
            std::make_pair(edge.dst_type, edge.dst_prop));
        column_prop_map[reversed_columns.at(edge.dst_edge_prop)] =
            graphar::Property(edge.dst_edge_prop, dst_prop.type,
                              dst_prop.is_primary, dst_prop.is_nullable);

        std::unordered_map<
            std::string,
            std::pair<std::string, std::shared_ptr<arrow::DataType>>>
            columns_to_change;
        for (const auto& [column, prop] : column_prop_map) {
          auto arrow_data_type =
              graphar::DataType::DataTypeToArrowDataType(prop.type);
          auto arrow_column = table->GetColumnByName(column);
          // TODO: is needed?
          if (!prop.is_nullable) {
            for (const auto& chunk : arrow_column->chunks()) {
              if (chunk->null_count() > 0) {
                throw std::runtime_error("Non-nullable column '" + column +
                                         "' has null values");
              }
            }
          }
          if (column != prop.name ||
              arrow_column->type()->id() != arrow_data_type->id()) {
            std::cout << "    Column \"" << column
                      << "\" will be converted to \"" << prop.name
                      << "\" with data type " << arrow_data_type->ToString()
                      << std::endl;
            columns_to_change[column] =
                std::make_pair(prop.name, arrow_data_type);
          }
        }
        table = ChangeNameAndDataType(table, columns_to_change);
        edge_tables.emplace_back(table);
      }
      std::unordered_map<
          std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>
          vertex_columns_to_change;

      std::shared_ptr<arrow::Table> merged_edge_table =
          MergeTables(edge_tables);
      std::cout << "  Merged into a single table with "
                << merged_edge_table->num_rows() << " rows and "
                << merged_edge_table->num_columns() << " columns." << std::endl;

      auto combined_edge_table =
          merged_edge_table->CombineChunks().ValueOrDie();
      std::cout << "  Combined chunks: " << combined_edge_table->num_rows()
                << " rows after combining." << std::endl;
      std::cout << "Edge validate level: " << edge.validate_level << std::endl;

      auto edge_builder =
          graphar::builder::EdgesBuilder::Make(
              edge_info, save_path_str, adj_list->GetType(), vertex_count,
              StringToValidateLevel(edge.validate_level))
              .value();
      std::cout << "  Created EdgesBuilder for adjacency list type: "
                << static_cast<int>(adj_list->GetType()) << std::endl;

      std::vector<std::string> edge_column_names;
      for (const auto& field : combined_edge_table->schema()->fields()) {
        edge_column_names.push_back(field->name());
      }
      const int64_t num_rows = combined_edge_table->num_rows();
      std::cout << "  Processing " << num_rows << " edge records..."
                << std::endl;
      for (int64_t i = 0; i < num_rows; ++i) {
        if (i % 10000 == 0 && i != 0) {
          std::cout << "    Processed " << i << " edges..." << std::endl;
        }
        auto edge_src_column =
            combined_edge_table->GetColumnByName(edge.src_edge_prop);
        auto edge_dst_column =
            combined_edge_table->GetColumnByName(edge.dst_edge_prop);

        auto src_scalar = edge_src_column->GetScalar(i).ValueOrDie();
        auto dst_scalar = edge_dst_column->GetScalar(i).ValueOrDie();
        auto src_key = std::make_pair(edge.src_type, edge.src_prop);
        auto dst_key = std::make_pair(edge.dst_type, edge.dst_prop);

        if (vertex_prop_index_map.find(src_key) ==
            vertex_prop_index_map.end()) {
          std::cout << "Source vertex property map not found for "
                    << edge.src_type << ", " << edge.src_prop << std::endl;
          continue;
        }
        if (vertex_prop_index_map.at(src_key).find(src_scalar) ==
            vertex_prop_index_map.at(src_key).end()) {
          std::cout << "Source vertex not found: type=" << edge.src_type
                    << ", prop=" << edge.src_prop
                    << ", value=" << src_scalar->ToString() << std::endl;
          continue;
        }
        if (vertex_prop_index_map.find(dst_key) ==
            vertex_prop_index_map.end()) {
          std::cout << "Destination vertex property map not found for "
                    << edge.dst_type << ", " << edge.dst_prop << std::endl;
          continue;
        }
        if (vertex_prop_index_map.at(dst_key).find(dst_scalar) ==
            vertex_prop_index_map.at(dst_key).end()) {
          std::cout << "Destination vertex not found: type=" << edge.dst_type
                    << ", prop=" << edge.dst_prop
                    << ", value=" << dst_scalar->ToString() << std::endl;
          continue;
        }

        graphar::builder::Edge e(
            vertex_prop_index_map.at(src_key).at(src_scalar),
            vertex_prop_index_map.at(dst_key).at(dst_scalar));
        for (const auto& column_name : edge_column_names) {
          if (column_name != edge.src_edge_prop &&
              column_name != edge.dst_edge_prop) {
            auto column = combined_edge_table->GetColumnByName(column_name);
            auto column_type = column->type();
            std::any value;
            TryToCastToAny(
                graphar::DataType::ArrowDataTypeToDataType(column_type),
                column->chunk(0), value, i);
            if (value.has_value()) {
              e.AddProperty(column_name, value);
            }
          }
        }
        edge_builder->AddEdge(e);
      }
      std::cout
          << "  Finished processing all edge records. Dumping builder data..."
          << std::endl;
      edge_builder->SetValidateLevel(
          StringToValidateLevel(edge.validate_level));
      edge_builder->Dump();
      std::cout << "  Finished dumping builder data." << std::endl;
    }
  }
  auto graph_info = std::make_shared<graphar::GraphInfo>(
      import_config.graphar_config.name, vertices_info, edges_info,
      vertices_labels, "./", version);
  auto file_name = graph_info->GetName() + ".yaml";
  graph_info->Save(save_path / file_name);

  return "Imported successfully!";
}

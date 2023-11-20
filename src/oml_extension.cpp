#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include <fstream>
#include <iostream>
#include <utility>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

struct oml_append_information {
    duckdb::unique_ptr<InternalAppender> appender;
};

void AppendValues(ClientContext &context, TableCatalogEntry &tbl_catalog, vector<Value> values) {
    auto append_info = make_uniq<oml_append_information>();
    append_info->appender = make_uniq<InternalAppender>(context, tbl_catalog);

    append_info->appender->BeginRow();
    for (size_t i = 0; i < values.size(); i++) {
        append_info->appender->Append(values[i]);
    }
    append_info->appender->EndRow();
}

struct DBGenFunctionData : public TableFunctionData {
	DBGenFunctionData() {
	}

	bool finished;
	string catalog;
	string schema;
    string filepath;
    string table;
    int start;
    vector<string> columns;
    vector<LogicalType> types;
};

static LogicalType Stringtype2LogicalType(const std::string& stringtype) {
    if (stringtype == "uint32")
        return LogicalType::UINTEGER;
    if (stringtype == "int32")
        return LogicalType::INTEGER;
    if (stringtype == "double")
        return LogicalType::DOUBLE;
    if (stringtype == "string")
        return LogicalType::VARCHAR;
    throw InternalException("Invalid type 1: " + stringtype);
}

static Value Stringvalue2Value(const std::string stringvalue, const LogicalType type) {
    if (type == LogicalType::UINTEGER)
        return Value::UINTEGER(std::stoi(stringvalue));
    if (type == LogicalType::INTEGER)
        return Value::INTEGER(std::stoi(stringvalue));
    if (type == LogicalType::DOUBLE)
        return Value::DOUBLE(std::stod(stringvalue));
    if (type == LogicalType::VARCHAR)
        return Value(stringvalue);
    throw InternalException("Invalid type 2: " + stringvalue + " " + type.ToString());
}

static void CreateTable(ClientContext &context, TableFunctionInput &data_p) {
    auto &data = (DBGenFunctionData &)*data_p.bind_data;

	auto info = make_uniq<CreateTableInfo>();
	info->catalog = data.catalog;
	info->schema = data.schema;
	info->table = data.table;
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	info->temporary = false;

	for (idx_t i = 0; i < data.columns.size(); i++) {
		info->columns.AddColumn(ColumnDefinition(data.columns[i], data.types[i]));
        info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
	}

	auto &catalog = Catalog::GetCatalog(context, data.catalog);
	catalog.CreateTable(context, std::move(info));
}

static void LoadOmlData(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (DBGenFunctionData &)*data_p.bind_data;

    std::ifstream file(data.filepath);

    if (!file.is_open()) {
        throw InternalException("Could not open file");
    }

    std::string line;

    for (int i = 0; i < data.start; i++) {
        if (!std::getline(file, line)) {
            throw InternalException("Expected metadata");
        }
    }

    auto &catalog = Catalog::GetCatalog(context, data.catalog);
    auto &tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, data.schema, data.table);
    auto appender = make_uniq<InternalAppender>(context, tbl_catalog);

    idx_t row_count = 0;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        vector<Value> values;

        int idx = 0;
        do {
            std::string value;
            iss >> value;
            if (!value.empty()) {
                values.push_back(Stringvalue2Value(value, data.types[idx]));
            }
            idx++;
        } while (iss);

        AppendValues(context, tbl_catalog, values);

        row_count++;
    }

    appender->Flush();
    appender.reset();

    output.SetValue(0, 0, Value::INTEGER(row_count));
    output.SetValue(1, 0, Value(data.table));
    output.SetCardinality(1);

    data.finished = true;
    file.close();
}

static duckdb::unique_ptr<FunctionData> OmlgenBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DBGenFunctionData>();

    std::string filepath = StringValue::Get(input.inputs[0]);
    
    result->finished = false;
    result->catalog = "memory";
    result->schema = "main";
    result->filepath = filepath;

    std::ifstream file(filepath);

    if (!file.is_open()) {
        throw InternalException("Could not open file");
    }

    string table;
    vector<string> columns;
    vector<LogicalType> types;
    int end = 0;

    string line;
    while (std::getline(file, line) && !line.empty()) {
        std::istringstream iss(line);
        std::string token;
        iss >> token;

        if (token == "schema:") {
            std::string columnInfo;
            while (iss >> columnInfo) {
                size_t colonPos = columnInfo.find(':');
                if (colonPos != std::string::npos) {
                    string columnName = columnInfo.substr(0, colonPos);
                    string columnType = columnInfo.substr(colonPos + 1);

                    columns.push_back(columnName);
                    types.push_back(Stringtype2LogicalType(columnType));
                } else {
                    table = columnInfo;
                }
            }
        }
        end++;
    }

    result->table = table;
    result->columns = columns;
    result->types = types;
    result->start = end + 1;

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("Rows inserted");

    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("Table name");

	return std::move(result);
}

static duckdb::unique_ptr<FunctionData> LoadgenBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DBGenFunctionData>();

    result->finished = false;
    result->catalog = "memory";
    result->schema = "main";
    result->filepath = StringValue::Get(input.inputs[0]);
    result->table = "Power_Consumption";
    result->start = 9;
    result->columns = {"experiment_id", "node_id", "node_id_seq", "time_sec", "time_usec", "power", "current", "voltage"};
    result->types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE,
                                        LogicalType::DOUBLE, LogicalType::DOUBLE};

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("Rows inserted");

    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("Table name");

	return std::move(result);
}

static void GenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DBGenFunctionData &)*data_p.bind_data;
    
	if (data.finished) {
		return;
	}

	CreateTable(context, data_p);
	LoadOmlData(context, data_p, output);

	data.finished = true;
}

static void LoadInternal(DatabaseInstance &instance) {
    TableFunction power_consumption_gen("Power_Consumption_load", {LogicalType::VARCHAR}, GenFunction, LoadgenBind);
	ExtensionUtil::RegisterFunction(instance, power_consumption_gen);

    TableFunction oml_gen("OmlGen", {LogicalType::VARCHAR}, GenFunction, OmlgenBind);
	ExtensionUtil::RegisterFunction(instance, oml_gen);
}

void OmlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string OmlExtension::Name() {
	return "oml";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void oml_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *oml_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

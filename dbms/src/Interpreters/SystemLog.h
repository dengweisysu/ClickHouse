#pragma once

#include <thread>
#include <atomic>
#include <condition_variable>
#include <boost/noncopyable.hpp>
#include <common/logger_useful.h>
#include <Core/Types.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Common/Stopwatch.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{


/** Allow to store structured log in system table.
  *
  * Logging is asynchronous. Data is put into queue from where it will be read by separate thread.
  * That thread inserts log into a table with no more than specified periodicity.
  */

/** Structure of log, template parameter.
  * Structure could change on server version update.
  * If on first write, existing table has different structure,
  *  then it get renamed (put aside) and new table is created.
  */
/* Example:
    struct LogElement
    {
        /// default constructor must be available
        /// fields

        static std::string name();
        static Block createBlock();
        void appendToBlock(Block & block) const;
    };
    */

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

#define DBMS_SYSTEM_LOG_QUEUE_SIZE 1048576

class Context;
class QueryLog;
class QueryThreadLog;
class PartLog;
class TextLog;
class TraceLog;
class MetricLog;

/// System logs should be destroyed in destructor of the last Context and before tables,
///  because SystemLog destruction makes insert query while flushing data into underlying tables
struct SystemLogs
{
    SystemLogs(Context & global_context, const Poco::Util::AbstractConfiguration & config);
    ~SystemLogs();

    void shutdown();

    std::shared_ptr<QueryLog> query_log;                /// Used to log queries.
    std::shared_ptr<QueryThreadLog> query_thread_log;   /// Used to log query threads.
    std::shared_ptr<PartLog> part_log;                  /// Used to log operations with parts
    std::shared_ptr<TraceLog> trace_log;                /// Used to log traces from query profiler
    std::shared_ptr<TextLog> text_log;                  /// Used to log all text messages.
    std::shared_ptr<MetricLog> metric_log;              /// Used to log all metrics.

    String part_log_database;
};


template <typename LogElement>
class SystemLog : private boost::noncopyable
{
public:
    using Self = SystemLog;

    /** Parameter: table name where to write log.
      * If table is not exists, then it get created with specified engine.
      * If it already exists, then its structure is checked to be compatible with structure of log record.
      *  If it is compatible, then existing table will be used.
      *  If not - then existing table will be renamed to same name but with suffix '_N' at end,
      *   where N - is a minimal number from 1, for that table with corresponding name doesn't exist yet;
      *   and new table get created - as if previous table was not exist.
      */
    SystemLog(
        Context & context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);

    ~SystemLog();

    /** Append a record into log.
      * Writing to table will be done asynchronously and in case of failure, record could be lost.
      */
    void add(const LogElement & element);

    /// Flush data in the buffer to disk
    void flush();

    /// Stop the background flush thread before destructor. No more data will be written.
    void shutdown();

protected:
    Logger * log;

private:
    Context & context;
    const String database_name;
    const String table_name;
    const String storage_def;
    StoragePtr table;
    const size_t flush_interval_milliseconds;

    enum class EntryType
    {
        LOG_ELEMENT = 0,
        AUTO_FLUSH,
        FORCE_FLUSH,
        SHUTDOWN,
    };

    /// Queue is bounded. But its size is quite large to not block in all normal cases.
    std::vector<LogElement> queue;
    uint64_t queue_front_index = 1;

    /** In this thread, data is pulled from 'queue' and stored in 'data', and then written into table.
      */
    ThreadFromGlobalPool saving_thread;

    void threadFunction();

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    bool is_prepared = false;
    void prepareTable();

    std::mutex mutex;
    bool is_shutdown = false;
    std::condition_variable flush_event;
    uint64_t last_requested_flush = 0;
    uint64_t last_flushed = 0;

    /// flushImpl can be executed only in saving_thread.
    void flushImpl(const std::vector<LogElement> & to_flush, uint64_t last_entry_index);
};


template <typename LogElement>
SystemLog<LogElement>::SystemLog(Context & context_,
    const String & database_name_,
    const String & table_name_,
    const String & storage_def_,
    size_t flush_interval_milliseconds_)
    : context(context_),
    database_name(database_name_), table_name(table_name_), storage_def(storage_def_),
    flush_interval_milliseconds(flush_interval_milliseconds_)
{
    log = &Logger::get("SystemLog (" + database_name + "." + table_name + ")");

    saving_thread = ThreadFromGlobalPool([this] { threadFunction(); });
}


template <typename LogElement>
void SystemLog<LogElement>::add(const LogElement & element)
{
    std::unique_lock lock(mutex);

    if (is_shutdown)
        return;

    if (queue.size() >= DBMS_SYSTEM_LOG_QUEUE_SIZE / 2)
    {
        // The queue more than half full, time to flush.
        const uint64_t last_entry = queue_front_index + queue.size() - 1;
        if (last_requested_flush < last_entry)
        {
            last_requested_flush = last_entry;
        }
        flush_event.notify_all();
    }

    if (queue.size() >= DBMS_SYSTEM_LOG_QUEUE_SIZE)
    {
        // TextLog sets its logger level to 0, so this log is a noop and there
        // is no recursive logging.
        LOG_ERROR(log, "Queue is full for system log '" + demangle(typeid(*this).name()) + "'.");
        return;
    }

    queue.push_back(element);
}


template <typename LogElement>
void SystemLog<LogElement>::flush()
{
    std::unique_lock lock(mutex);

    if (is_shutdown)
        return;

    const uint64_t entry_index = queue_front_index + queue.size() - 1;

    if (last_requested_flush < entry_index)
    {
        last_requested_flush = entry_index;
    }
    flush_event.notify_all();

    const int timeout_seconds = 60;
    bool result = flush_event.wait_for(lock, std::chrono::seconds(timeout_seconds),
        [&] { return last_flushed >= entry_index; });

    if (!result)
    {
        throw Exception("Timeout exceeded (" + toString(timeout_seconds) + " s) while flushing system log '" + demangle(typeid(*this).name()) + "'.",
            ErrorCodes::TIMEOUT_EXCEEDED);
    }
}


template <typename LogElement>
void SystemLog<LogElement>::shutdown()
{
    {
        std::unique_lock lock(mutex);

        if (is_shutdown)
        {
            return;
        }

        is_shutdown = true;

        /// Tell thread to shutdown.
        flush_event.notify_all();
    }

    saving_thread.join();
}


template <typename LogElement>
SystemLog<LogElement>::~SystemLog()
{
    shutdown();
}


template <typename LogElement>
void SystemLog<LogElement>::threadFunction()
{
    setThreadName("SystemLogFlush");

    bool exit_this_thread = false;
    while (!exit_this_thread)
    {
        try
        {
            std::vector<LogElement> to_flush;
            uint64_t last_entry_index = 0;

            {
                std::unique_lock lock(mutex);
                flush_event.wait_for(lock, std::chrono::milliseconds(flush_interval_milliseconds),
                                     [&] () { return last_requested_flush > last_flushed || is_shutdown; });

                queue_front_index += queue.size();
                last_entry_index = queue_front_index - 1;
                queue.swap(to_flush);

                exit_this_thread = is_shutdown;
            }

            if (to_flush.empty())
            {
                continue;
            }

            flushImpl(to_flush, last_entry_index);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


template <typename LogElement>
void SystemLog<LogElement>::flushImpl(const std::vector<LogElement> & to_flush, uint64_t last_entry_index)
{
    try
    {
        LOG_TRACE(log, "Flushing system log");

        /// We check for existence of the table and create it as needed at every flush.
        /// This is done to allow user to drop the table at any moment (new empty table will be created automatically).
        /// BTW, flush method is called from single thread.
        prepareTable();

        Block block = LogElement::createBlock();
        for (const auto & elem : to_flush)
            elem.appendToBlock(block);

        /// We write to table indirectly, using InterpreterInsertQuery.
        /// This is needed to support DEFAULT-columns in table.

        std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
        insert->database = database_name;
        insert->table = table_name;
        ASTPtr query_ptr(insert.release());

        InterpreterInsertQuery interpreter(query_ptr, context);
        BlockIO io = interpreter.execute();

        io.out->writePrefix();
        io.out->write(block);
        io.out->writeSuffix();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    std::unique_lock lock(mutex);
    last_flushed = last_entry_index;
    flush_event.notify_all();
}


template <typename LogElement>
void SystemLog<LogElement>::prepareTable()
{
    String description = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name);

    table = context.tryGetTable(database_name, table_name);

    if (table)
    {
        const Block expected = LogElement::createBlock();
        const Block actual = table->getSampleBlockNonMaterialized();

        if (!blocksHaveEqualStructure(actual, expected))
        {
            /// Rename the existing table.
            int suffix = 0;
            while (context.isTableExist(database_name, table_name + "_" + toString(suffix)))
                ++suffix;

            auto rename = std::make_shared<ASTRenameQuery>();

            ASTRenameQuery::Table from;
            from.database = database_name;
            from.table = table_name;

            ASTRenameQuery::Table to;
            to.database = database_name;
            to.table = table_name + "_" + toString(suffix);

            ASTRenameQuery::Element elem;
            elem.from = from;
            elem.to = to;

            rename->elements.emplace_back(elem);

            LOG_DEBUG(log, "Existing table " << description << " for system log has obsolete or different structure."
            " Renaming it to " << backQuoteIfNeed(to.table));

            InterpreterRenameQuery(rename, context).execute();

            /// The required table will be created.
            table = nullptr;
        }
        else if (!is_prepared)
            LOG_DEBUG(log, "Will use existing table " << description << " for " + LogElement::name());
    }

    if (!table)
    {
        /// Create the table.
        LOG_DEBUG(log, "Creating new table " << description << " for " + LogElement::name());

        auto create = std::make_shared<ASTCreateQuery>();

        create->database = database_name;
        create->table = table_name;

        Block sample = LogElement::createBlock();

        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns, InterpreterCreateQuery::formatColumns(sample.getNamesAndTypesList()));
        create->set(create->columns_list, new_columns_list);

        ParserStorage storage_parser;
        ASTPtr storage_ast = parseQuery(
            storage_parser, storage_def.data(), storage_def.data() + storage_def.size(),
            "Storage to create table for " + LogElement::name(), 0);
        create->set(create->storage, storage_ast);

        InterpreterCreateQuery interpreter(create, context);
        interpreter.setInternal(true);
        interpreter.execute();

        table = context.getTable(database_name, table_name);
    }

    is_prepared = true;
}

}

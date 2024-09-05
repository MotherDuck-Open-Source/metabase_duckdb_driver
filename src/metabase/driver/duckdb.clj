(ns metabase.driver.duckdb
  (:require 
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [java-time.api :as t] 
   [medley.core :as m] 
   [metabase.driver :as driver] 
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common] 
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn] 
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute] 
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync] 
   [metabase.driver.sql.query-processor :as sql.qp] 
   [metabase.models.secret :as secret] 
   [metabase.public-settings.premium-features :as premium-features]
   [metabase.util.honey-sql-2 :as h2x])
  (:import 
   (java.sql PreparedStatement ResultSet ResultSetMetaData Statement Time Types)
   (java.time LocalDate LocalTime OffsetTime)
   (java.time.temporal ChronoField)))

(set! *warn-on-reflection* true)

(driver/register! :duckdb, :parent :sql-jdbc)

(defn- jdbc-spec
  "Creates a spec for `clojure.java.jdbc` to use for connecting to DuckDB via JDBC from the given `opts`"
  [{:keys [database_file, old_implicit_casting, motherduck_token], :as details}]
  (when (not (seq (re-find #"^md:" database_file))) 
    (throw (ex-info "Metabase Cloud requires MotherDuck connection string, local duckdb file or in-memory connection not supported. " {:database_file database_file}))) 
  (-> details 
      (merge
       {:classname         "org.duckdb.DuckDBDriver"
        :subprotocol       "duckdb"
        :subname           (or database_file "")
        "custom_user_agent" (str "metabase" (if (premium-features/is-hosted?) " metabase-cloud" ""))
        "temp_directory"   (str database_file ".tmp")
        "jdbc_stream_results" "true"}
       (when old_implicit_casting
         {"old_implicit_casting" (str old_implicit_casting)})
       {"motherduck_attach_mode"  "single"   ;; when connecting to MotherDuck, explicitly connect to a single database
        "motherduck_saas_mode"    "true"}    ;; saas mode is required to limit what the DuckDB instance can do
       (when (seq motherduck_token)     ;; Only configure the option if token is provided
         {"motherduck_token" motherduck_token}))
      (dissoc :database_file :port :engine :old_implicit_casting :motherduck_token) 
      sql-jdbc.common/handle-additional-options))

(defn- remove-keys-with-prefix [details prefix]
  (apply dissoc details (filter #(str/starts-with? (name %) prefix) (keys details))))

(defmethod sql-jdbc.conn/connection-details->spec :duckdb
  [_ details-map] 
  (-> details-map 
      (merge {:motherduck_token (or (-> (secret/db-details-prop->secret-map details-map "motherduck_token") 
                                        secret/value->string) 
                                    (secret/get-secret-string details-map "motherduck_token"))})
      (remove-keys-with-prefix "motherduck_token-")
      jdbc-spec))

(defmethod sql-jdbc.execute/set-timezone-sql :duckdb [_]
  "SET GLOBAL TimeZone=%s;")

(def ^:private database-type->base-type 
  (sql-jdbc.sync/pattern-based-database-type->base-type 
   [[#"BOOLEAN"                  :type/Boolean]
    [#"BOOL"                     :type/Boolean]
    [#"LOGICAL"                  :type/Boolean]
    [#"HUGEINT"                  :type/BigInteger]
    [#"UBIGINT"                  :type/BigInteger]
    [#"BIGINT"                   :type/BigInteger]
    [#"INT8"                     :type/BigInteger]
    [#"LONG"                     :type/BigInteger]
    [#"INT4"                     :type/Integer]
    [#"SIGNED"                   :type/Integer]
    [#"INT2"                     :type/Integer]
    [#"SHORT"                    :type/Integer]
    [#"INT1"                     :type/Integer]
    [#"UINTEGER"                 :type/Integer]
    [#"USMALLINT"                :type/Integer]
    [#"UTINYINT"                 :type/Integer]
    [#"INTEGER"                  :type/Integer]
    [#"SMALLINT"                 :type/Integer]
    [#"TINYINT"                  :type/Integer]
    [#"INT"                      :type/Integer]
    [#"DECIMAL"                  :type/Decimal]
    [#"DOUBLE"                   :type/Float]
    [#"FLOAT8"                   :type/Float]
    [#"NUMERIC"                  :type/Float]
    [#"REAL"                     :type/Float]
    [#"FLOAT4"                   :type/Float]
    [#"FLOAT"                    :type/Float]
    [#"VARCHAR"                  :type/Text]
    [#"BPCHAR"                   :type/Text]
    [#"CHAR"                     :type/Text]
    [#"TEXT"                     :type/Text]
    [#"STRING"                   :type/Text]
    [#"BLOB"                     :type/*]
    [#"BYTEA"                    :type/*]
    [#"VARBINARY"                :type/*]
    [#"BINARY"                   :type/*]
    [#"UUID"                     :type/UUID]
    [#"TIMESTAMPTZ"              :type/DateTimeWithTZ]
    [#"TIMESTAMP WITH TIME ZONE" :type/DateTimeWithTZ]
    [#"DATETIME"                 :type/DateTime]
    [#"TIMESTAMP_S"              :type/DateTime]
    [#"TIMESTAMP_MS"             :type/DateTime]
    [#"TIMESTAMP_NS"             :type/DateTime]
    [#"TIMESTAMP"                :type/DateTime]
    [#"DATE"                     :type/Date]
    [#"TIME"                     :type/Time]]))

(defmethod sql-jdbc.sync/database-type->base-type :duckdb
  [_ field-type]
  (database-type->base-type field-type))


(defn- local-time-to-time [^LocalTime lt]
  (Time. (.getLong lt ChronoField/MILLI_OF_DAY)))

(defmethod sql-jdbc.execute/set-parameter [:duckdb LocalDate]
  [_ ^PreparedStatement prepared-statement i t]
  (.setObject prepared-statement i (t/local-date-time t (t/local-time 0))))

(defmethod sql-jdbc.execute/set-parameter [:duckdb LocalTime]
  [_ ^PreparedStatement prepared-statement i t]
  (.setObject prepared-statement i (local-time-to-time t)))

(defmethod sql-jdbc.execute/set-parameter [:duckdb OffsetTime]
  [_ ^PreparedStatement prepared-statement i ^OffsetTime t]
  (let [adjusted-tz  (local-time-to-time (t/local-time (t/with-offset-same-instant t (t/zone-offset 0))))]
    (.setObject prepared-statement i adjusted-tz)))

(defmethod sql-jdbc.execute/set-parameter [:duckdb String]
  [_ ^PreparedStatement prepared-statement i t]
  (.setObject prepared-statement i t))


;; .getObject of DuckDB (v0.4.0) does't handle the java.time.LocalDate but sql.Date only,
;; so get the sql.Date from DuckDB and convert it to java.time.LocalDate
(defmethod sql-jdbc.execute/read-column-thunk [:duckdb Types/DATE]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [sqlDate (.getDate rs i)]
      (.toLocalDate sqlDate))))

;; .getObject of DuckDB (v0.4.0) does't handle the java.time.LocalTime but sql.Time only,
;; so get the sql.Time from DuckDB and convert it to java.time.LocalTime
(defmethod sql-jdbc.execute/read-column-thunk [:duckdb Types/TIME]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [sql-time-string (.getString rs i)]
      (LocalTime/parse sql-time-string))))

;; override the sql-jdbc.execute/read-column-thunk for TIMESTAMP based on
;; DuckDB JDBC implementation.
(defmethod sql-jdbc.execute/read-column-thunk [:duckdb Types/TIMESTAMP]
   [_ ^ResultSet rs _ ^Integer i]
    (fn []
     (when-let [t (.getTimestamp rs i)]
       (t/local-date-time t))))

;; date processing for aggregation
(defmethod driver/db-start-of-week :duckdb [_] :monday)

(defmethod sql.qp/add-interval-honeysql-form :duckdb
  [driver hsql-form amount unit]
  (if (= unit :quarter)
    (recur driver hsql-form (* amount 3) :month)
    (h2x/+ (h2x/->timestamp-with-time-zone hsql-form) [:raw (format "(INTERVAL '%d' %s)" (int amount) (name unit))])))

(defmethod sql.qp/date [:duckdb :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:duckdb :minute]          [_ _ expr] [:date_trunc (h2x/literal :minute) expr])
(defmethod sql.qp/date [:duckdb :minute-of-hour]  [_ _ expr] [:minute expr])
(defmethod sql.qp/date [:duckdb :hour]            [_ _ expr] [:date_trunc (h2x/literal :hour) expr])
(defmethod sql.qp/date [:duckdb :hour-of-day]     [_ _ expr] [:hour expr])
(defmethod sql.qp/date [:duckdb :day]             [_ _ expr] [:date_trunc (h2x/literal :day) expr])
(defmethod sql.qp/date [:duckdb :day-of-month]    [_ _ expr] [:day expr])
(defmethod sql.qp/date [:duckdb :day-of-year]     [_ _ expr] [:dayofyear expr])

(defmethod sql.qp/date [:duckdb :day-of-week]
  [driver _ expr]
  (sql.qp/adjust-day-of-week driver [:isodow expr]))

(defmethod sql.qp/date [:duckdb :week]
  [driver _ expr]
  (sql.qp/adjust-start-of-week driver (partial conj [:date_trunc] (h2x/literal :week)) expr))

(defmethod sql.qp/date [:duckdb :month]           [_ _ expr] [:date_trunc (h2x/literal :month) expr])
(defmethod sql.qp/date [:duckdb :month-of-year]   [_ _ expr] [:month expr])
(defmethod sql.qp/date [:duckdb :quarter]         [_ _ expr] [:date_trunc (h2x/literal :quarter) expr])
(defmethod sql.qp/date [:duckdb :quarter-of-year] [_ _ expr] [:quarter expr])
(defmethod sql.qp/date [:duckdb :year]            [_ _ expr] [:date_trunc (h2x/literal :year) expr])

(defmethod sql.qp/unix-timestamp->honeysql [:duckdb :seconds]
  [_ _ expr]
  [:to_timestamp (h2x/cast :DOUBLE expr)])

(defmethod sql.qp/->honeysql [:duckdb :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) 0])

;; empty result set for queries without result (like insert...)
(defn- empty-rs []
  (reify
    ResultSet
    (getMetaData [_]
      (reify
        ResultSetMetaData
        (getColumnCount [_] 1)
        (getColumnLabel [_ _idx] "WARNING")
        (getColumnTypeName [_ _] "CHAR")
        (getColumnType [_ _] Types/CHAR)))
    (next [_] false)
    (close [_])))

;; override native execute-statement! to make queries that does't returns ResultSet

(defmethod sql-jdbc.execute/execute-statement! :duckdb
  [_driver ^Statement stmt ^String sql]
  (if (.execute stmt sql)
    (.getResultSet stmt)
    (empty-rs)))

(defmethod driver/describe-database :duckdb
  [driver database]
  (let [get_tables_query "select * from information_schema.tables"]
    {:tables
     (sql-jdbc.execute/do-with-connection-with-options
      driver database nil
      (fn [conn]
        (set
         (for [{:keys [table_schema table_name]}
               (jdbc/query {:connection conn}
                           [get_tables_query])]
           {:name table_name :schema table_schema}))))}))



(defmethod driver/describe-table :duckdb
  [driver database {table_name :name, schema :schema}]
  (let [get_columns_query (format 
                           "select * from information_schema.columns where table_name = '%s' and table_schema = '%s'" 
                           table_name schema)] 
    {:name   table_name
     :schema schema
     :fields
     (sql-jdbc.execute/do-with-connection-with-options
      driver database nil
      (fn [conn] (let [results (jdbc/query
                                {:connection conn}
                                [get_columns_query])]
                   (set
                    (for [[idx {column_name :column_name, data_type :data_type}] (m/indexed results)]
                      {:name              column_name
                       :database-type     data_type
                       :base-type         (sql-jdbc.sync/database-type->base-type driver (keyword data_type))
                       :database-position idx})))))}))

;; The 0.4.0 DuckDB JDBC .getImportedKeys method throws 'not implemented' yet.
;; There is no support of FK yet.
(defmethod driver/describe-table-fks :duckdb
  [_ _ _]
  nil)

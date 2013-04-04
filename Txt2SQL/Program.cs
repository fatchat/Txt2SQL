using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommandLine;
using CommandLine.Text;
using System.Data.SqlClient;

namespace Txt2SQL
{
    // ==============================================================================================================
    class Options
    {
        [Option('v', "verbose", DefaultValue = false, HelpText = "show details")]
        public bool verbose { get; set; }

        [Option('f', "file", Required = true, HelpText = "input file")]
        public string inputFile { get; set; }

        // ========= display the input data ==========
        [Option('r', "read-only", DefaultValue=false, HelpText="specify to stop after reading")]
        public bool read_only { get; set; }

        [Option('o', "timeformat", DefaultValue = "HH:mm:ss", HelpText = "datetime format when displaying input")]
        public string timeformat { get; set; }

        [Option('m', "transform-format", DefaultValue = null, HelpText = "Transform output, <TimeSeriesDBInsert>, <UnixTime>")]
        public string transform_format { get; set; }

        // ======= for display & DB writes ========
        [Option('b', "bucket-size", DefaultValue = 0, HelpText = "vertical width to define buckets for classifying input")]
        public float bucket_size { get; set; }

        [Option('d', "diffs", DefaultValue = false, HelpText = "calc adjacent differences")]
        public bool diffs { get; set; }

        // ========= for DB writes only ==========
        [Option('h', "history", HelpText = "length of history")]
        public int history { get; set; }

        [Option('p', "predict", HelpText = "length of prediction window")]
        public int predict { get; set; }

        [Option('n', "dbname", HelpText = "name of DB to create table in")]
        public string dbname { get; set; }

        [Option('t', "table", HelpText = "name of SQL table to create")]
        public string tablename { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
                                      (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }

    // ==============================================================================================================
    class DBTable
    {
        private string tableName_;
        private int history_size_;
        private int prediction_size_;
        private string insertCmd_;

        public bool verbose { get; set; }

        public DBTable(SqlConnection sqlConnection, string tableName, int history_size, int prediction_size, int timestep)
        {
            this.tableName_ = tableName;
            this.history_size_ = history_size;
            this.prediction_size_ = prediction_size;
            this.verbose = true;
            List<string> column_names = (from column in Enumerable.Range(1, history_size_)
                                select string.Format("history_{0}", timestep * (history_size_ - column))
                               ).ToList<string>();
            column_names.AddRange(from column in Enumerable.Range(1, prediction_size_)
                                  select string.Format("predict_{0}", timestep * column)
                                  );
            string create_cmd = "CREATE TABLE " + 
                                tableName_ + 
                                " (key_col [nchar](10) NOT NULL," +
                                string.Join(", ", from col_name in column_names
                                                 select string.Format("{0} [float] NOT NULL", col_name)) + 
                                ")";
            if (this.verbose)
                Console.WriteLine("Executing command {0}", create_cmd);
            try
            {
                SqlCommand createTableCmd = new SqlCommand(create_cmd, sqlConnection);
                createTableCmd.ExecuteNonQuery();
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                Console.WriteLine("Could not create table: {0}", e.Message);
                throw;
            }
            // create the INSERT statement head for use in Insert()
            insertCmd_ = "INSERT INTO " +
                         tableName_ +
                         " (key_col, " +
                         string.Join(", ", column_names) +
                         ") VALUES ";
        } 

        public void Insert(SqlConnection sqlConnection, IList<TickData> readings, int running_index)
        {
            string this_insert_cmd = insertCmd_ +
                                     string.Format("({0}, ", running_index) +
                                     string.Join(", ", from reading in readings select reading.data.ToString()) +
                                     ")";
            if (verbose)
                Console.WriteLine("Executing command {0}", this_insert_cmd);
            SqlCommand insertCmd = new SqlCommand(this_insert_cmd, sqlConnection);
            insertCmd.ExecuteNonQuery(); // may throw exception
        }
    }

    // ==============================================================================================================
    struct TickData
    {
        public DateTime dt { get; set; }
        public float data { get; set; }
    }
    enum TransformFormats
    {
        NoOutput,
        TimeSeriesDBInsert,
        UnixTime
    }
    struct ReadInputArgs
    {
        public bool diffs { get; set; }
        public float vert_width { get; set; }
        public string timeformat { get; set; }
        public string dbname { get; set; }
        public TransformFormats transform_format { get; set; }
    }
    public class Program
    {
        static bool sVerbose { get; set; }

        static IList<TickData> ReadInput(string inputFile, ReadInputArgs args)
        {
            string[] lines = System.IO.File.ReadAllLines(inputFile);
            IList<TickData> readings = new List<TickData>();
            const int timestamp_length = 11; // MAGIC NUMBER
            float last_val = 0;
            int counter = 0;
            foreach (string line in lines)
            {
                // each line looks like
                // [63500083637, 17.04221],
                string ts_str = line.Substring(1, timestamp_length);
                long timestamp = long.Parse(ts_str);
                DateTime datetime = new DateTime(timestamp * TimeSpan.TicksPerSecond);
                string tval = line.Substring(line.IndexOf(' ') + 1);
                string value = tval.Substring(0, tval.Length - 2);
                float reading = float.Parse(value);
                float new_val = reading;
                if (args.vert_width > 0)
                {
                    int bucket = (int)(reading / args.vert_width);
                    new_val = (float)bucket;
                }
                if (args.diffs)
                {
                    float orig_val = new_val;
                    new_val = new_val - last_val;
                    last_val = orig_val;
                }
                readings.Add(new TickData { dt = datetime, data = new_val });
                if (args.transform_format == TransformFormats.TimeSeriesDBInsert)
                {
                    Console.WriteLine("insert into dbo.{3} ([key_col], [dt], [value]) values ({2}, \'{0}\', {1});", 
                                        datetime.ToString(args.timeformat), 
                                        new_val, 
                                        counter.ToString(), 
                                        args.dbname);
                }
                if (args.transform_format == TransformFormats.UnixTime)
                {
                    long epoch_time = (datetime.ToUniversalTime().Ticks - 621355968000000000) / 10000000;
                    Console.WriteLine("[{0}, {1}],", epoch_time, new_val);
                }
                ++counter;
            }
            return readings;
        }

        static SqlConnection GetSqlConnection(string dbName)
        {
            SqlConnection sqlConnection = new SqlConnection();
            sqlConnection.ConnectionString = string.Format("Server=localhost; database={0}; Trusted_Connection=SSPI", dbName);
            try
            {
                if (sVerbose)
                    Console.WriteLine("Connecting using {0}", sqlConnection.ConnectionString);
                sqlConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not open connection to DB: " + e.Message);
                sqlConnection = null;
            }
            return sqlConnection;
        }

        static void Main(string[] args)
        {
            Options options = new Options();

            if (CommandLine.Parser.Default.ParseArguments(args, options))
            {
                sVerbose = options.verbose;
                // read values into a list of floats
                TransformFormats trans_format = TransformFormats.NoOutput;
                if (options.transform_format != null)
                {
                    if (!options.read_only)
                    {
                        Console.WriteLine("The transform-format flag must be accompanied by the read-only flag");
                        return;
                    }
                    if (Enum.IsDefined(typeof(TransformFormats), options.transform_format))
                    {
                        trans_format = (TransformFormats)Enum.Parse(typeof(TransformFormats), options.transform_format);
                    }
                    else
                    {
                        Console.WriteLine("Unrecognized transform format: {0}", options.transform_format);
                        return;
                    }
                }
                IList<TickData> readings = ReadInput(options.inputFile, new ReadInputArgs() 
                                                                        {
                                                                            vert_width=options.bucket_size
                                                                            , diffs=options.diffs
                                                                            , timeformat=options.timeformat
                                                                            , dbname=options.dbname
                                                                            , transform_format=trans_format
                                                                        });
                if (options.read_only)
                {
                    return;
                }
                if (options.predict < 1 || options.history < 1)
                {
                    Console.WriteLine("Values for <predict> and <history> must be at least 1");
                    return;
                }
                if (options.tablename == null)
                {
                    Console.WriteLine("Table name is required");
                    return;
                }
                // open connection to DB
                SqlConnection sqlConnection = GetSqlConnection(options.dbname);
                if (sqlConnection != null)
                {
                    try
                    {
                        // timestep is used to name the columns, we figure it out using only the first two values
                        int timestep = (readings[1].dt - readings[0].dt).Seconds;
                        // create a new table
                        DBTable dbTable = new DBTable(sqlConnection, options.tablename, options.history, options.predict, timestep);
                        dbTable.verbose = sVerbose;
                        // insert row by row
                        for (int index = 0; index + options.history + options.predict <= readings.Count; ++index)
                        {
                            List<TickData> sublist = ((List<TickData>)readings).GetRange(index, options.history + options.predict); // shallow copy
                            dbTable.Insert(sqlConnection, sublist, index);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Caught exception: {0}", e.Message);
                    }
                }
            }
        }
    }
}

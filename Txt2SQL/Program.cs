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
        [Option('f', "file", Required = true, HelpText = "input file")]
        public string inputFile { get; set; }

        [Option('r', "readonly", DefaultValue=false, HelpText="specify to stop after reading")]
        public bool read_only { get; set; }

        [Option('h', "history", HelpText="length of history")]
        public int history { get; set; }

        [Option('p', "predict", HelpText = "length of prediction window")]
        public int predict { get; set; }

        [Option('t', "table", HelpText = "name of SQL table to create")]
        public string table { get; set; }

        [Option('b', "bucket-size", DefaultValue=0, HelpText = "vertical width to define buckets for classifying input")]
        public float bucket_size { get; set; }

        [Option('v', "verbose", DefaultValue = false, HelpText = "Show details")]
        public bool verbose { get; set; }

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
            this.verbose = false;
            List<string> column_names = new List<string>();

            string create_cmd = "CREATE TABLE " + tableName_ + "(key_col [nchar](10) NOT NULL,";
            for (int column = 1; column <= history_size_; ++column)
            {
                string column_name = string.Format("history_{0}", timestep * (history_size_ - column));
                create_cmd += string.Format("{0} [float] NOT NULL,", column_name);
                column_names.Add(column_name);
            }
            for (int column = 1; column < prediction_size_; ++column)
            {
                string column_name = string.Format("predict_{0}", timestep * column);
                create_cmd += string.Format("{0} [float] NOT NULL,", column_name);
                column_names.Add(column_name);
            }
            {
                string column_name = string.Format("predict_{0}", timestep * prediction_size_);
                create_cmd += string.Format("{0} [float] NOT NULL)", column_name);
                column_names.Add(column_name);
            }
            try
            {
                if (this.verbose)
                    Console.WriteLine("Executing command {0}", create_cmd);
                SqlCommand createTableCmd = new SqlCommand(create_cmd, sqlConnection);
                createTableCmd.ExecuteNonQuery();
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                Console.WriteLine("Could not create table: {0}", e.Message);
                throw;
            }

            // create the INSERT statement head
            insertCmd_ = "INSERT INTO " + tableName_ + " (key_col, ";
            for (int column = 0; column < history_size_ + prediction_size_ - 1; ++column)
            {
                insertCmd_ += string.Format("{0},", column_names[column]);
            }
            insertCmd_ += string.Format("{0}) ", column_names.Last());
            insertCmd_ += "VALUES ";
        }

        public bool Insert(SqlConnection sqlConnection, IList<TickData> readings, int running_index)
        {
            string this_insert_cmd = insertCmd_;
            this_insert_cmd += string.Format("({0},", running_index);
            int last_index = history_size_ + prediction_size_ - 1;

            for (int index = 0; index < last_index; ++index)
            {
                this_insert_cmd += string.Format("{0},", readings[index].data);
            }
            this_insert_cmd += string.Format("{0})", readings[last_index].data);
            try
            {
                if (verbose)
                    Console.WriteLine("Executing command {0}", this_insert_cmd);
                SqlCommand insertCmd = new SqlCommand(this_insert_cmd, sqlConnection);
                insertCmd.ExecuteNonQuery();
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                Console.WriteLine("Could not insert into table: {0}", e.Message);
                return false;
            }
            return true;
        }
    }

    // ==============================================================================================================
    struct TickData
    {
        public DateTime dt { get; set; }
        public float data { get; set; }
    }
    public class Program
    {
        static bool verbose { get; set; }

        static IList<TickData> ReadInput(string inputFile, float vert_width)
        {
            string[] lines = System.IO.File.ReadAllLines(inputFile);
            IList<TickData> readings = new List<TickData>();
            const int timestamp_length = 11;

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
                if (vert_width > 0)
                {
                    int bucket = (int)(reading / vert_width);
                    new_val = (float)bucket;
                }
                readings.Add(new TickData { dt = datetime, data = new_val });
                if (verbose)
                    Console.WriteLine("{0} => {1}", datetime.ToString("HH:mm:ss"), new_val);
            }
            return readings;
        }

        static SqlConnection GetSqlConnection(string dbName)
        {
            SqlConnection sqlConnection = new SqlConnection();
            sqlConnection.ConnectionString = string.Format("Server=localhost; database={0}; Trusted_Connection=SSPI", dbName);
            try
            {
                if (verbose)
                    Console.WriteLine("Connecting using {0}", sqlConnection.ConnectionString);
                sqlConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not open connection to DB: " + e.ToString());
                sqlConnection = null;
            }
            return sqlConnection;
        }

        static void Main(string[] args)
        {
            Options options = new Options();

            if (CommandLine.Parser.Default.ParseArguments(args, options))
            {
                verbose = options.verbose;
                // read values into a list of floats
                IList<TickData> readings = ReadInput(options.inputFile, options.bucket_size);
                if (options.read_only)
                {
                    return;
                }
                if (options.predict < 1 || options.history < 1)
                {
                    Console.WriteLine("Values for <predict> and <history> must be at least 1");
                    return;
                }
                if (options.table == null)
                {
                    Console.WriteLine("Table name is required");
                    return;
                }
                // open connection to DB
                SqlConnection sqlConnection = GetSqlConnection("TimeSeries");
                if (sqlConnection != null)
                {
                    try
                    {
                        int timestep = (readings[1].dt - readings[0].dt).Seconds;
                        // create a new table
                        DBTable dbTable = new DBTable(sqlConnection, options.table, options.history, options.predict, timestep);
                        dbTable.verbose = verbose;
                        // insert row by row
                        for (int index = 0; index + options.history + options.predict <= readings.Count; ++index)
                        {
                            List<TickData> sublist = ((List<TickData>)readings).GetRange(index, options.history + options.predict);
                            if (false == dbTable.Insert(sqlConnection, sublist, index))
                                break;
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

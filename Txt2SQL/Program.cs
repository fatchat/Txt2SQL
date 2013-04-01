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

        [Option('h', "history", Required=true, HelpText="length of history")]
        public int history { get; set; }

        [Option('p', "predict", Required = true, HelpText = "length of prediction window")]
        public int predict { get; set; }

        [Option('t', "table", Required = true, HelpText = "name of SQL table to create")]
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
    public class Program
    {
        static IList<float> ReadInput(string inputFile, float vert_width)
        {
            string[] lines = System.IO.File.ReadAllLines(inputFile);
            IList<float> readings = new List<float>();

            foreach (string line in lines)
            {
                // each line looks like
                // [63500083637, 17.04221],
                string tail = line.Substring(1 + line.IndexOf(' '));
                string head = tail.Substring(0, tail.Length - 2);
                float reading = float.Parse(head);
                float new_val;

                if (vert_width > 0)
                {
                    int bucket = (int)(reading / vert_width);
                    new_val = (float)bucket;
                }
                else 
                {
                    new_val = reading; 
                }

                readings.Add(new_val);
            }

            return readings;
        }

        static void Main(string[] args)
        {
            Options options = new Options();

            if (CommandLine.Parser.Default.ParseArguments(args, options))
            {
                if (options.predict < 1 || options.history < 1)
                    throw new System.ArgumentOutOfRangeException("Values for <predict> and <history> must be at least 1");

                // ============================================================
                // read values into a list of floats
                IList<float> readings = ReadInput(options.inputFile, options.bucket_size);
         
                // ============================================================
                // write the readings into the SQL table
                SqlConnection sqlConnection = new SqlConnection();
                sqlConnection.ConnectionString = "Server=localhost; database=TimeSeries; Trusted_Connection=SSPI";

                try
                {
                    if (options.verbose)
                        Console.WriteLine("Connecting using {0}", sqlConnection.ConnectionString);
                    sqlConnection.Open();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Could not open connection to DB: " + e.ToString());
                }

                // create the table
                string table_name = options.table;
                string create_cmd = "CREATE TABLE " + table_name + "(key_col [nchar](10) NOT NULL,";
                for (int column = 1; column <= options.history; ++column)
                {
                    create_cmd += string.Format("history_{0} [float] NOT NULL,", column);
                }
                for (int column = 1; column < options.predict; ++column)
                {
                    create_cmd += string.Format("predict_{0} [float] NOT NULL,", column);
                }
                create_cmd += string.Format("predict_{0} [float] NOT NULL)", options.predict);
                
                try
                {
                    if (options.verbose)
                        Console.WriteLine("Executing command {0}", create_cmd);
                    SqlCommand createTableCmd = new SqlCommand(create_cmd, sqlConnection);
                    createTableCmd.ExecuteNonQuery();
                }
                catch (System.Data.SqlClient.SqlException e)
                {
                    Console.WriteLine("Could not create table: {0}", e.Message);
                    return;
                }

                // create the INSERT statement
                string insert_cmd = "INSERT INTO " + table_name + " (key_col, ";
                for (int column = 1; column <= options.history; ++column)
                {
                    insert_cmd += string.Format("history_{0},", column);
                }
                for (int column = 1; column < options.predict; ++column)
                {
                    insert_cmd += string.Format("predict_{0},", column);
                }
                insert_cmd += string.Format("predict_{0}) ", options.predict);
                insert_cmd += "VALUES ";

                for (int running_index = 0; running_index < readings.Count - options.history - options.predict + 1; ++running_index)
                {
                    string this_insert_cmd = insert_cmd;
                    this_insert_cmd += string.Format("({0},", running_index);

                    for (int index = 0; index < options.history + options.predict - 1; ++index)
                    {
                        this_insert_cmd += string.Format("{0},", readings[running_index + index]);
                    }
                    this_insert_cmd += string.Format("{0})", readings[running_index + options.history + options.predict - 1]);

                    try
                    {
                        if (options.verbose)
                            Console.WriteLine("Executing command {0}", this_insert_cmd);
                        SqlCommand insertCmd = new SqlCommand(this_insert_cmd, sqlConnection);
                        insertCmd.ExecuteNonQuery();
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        Console.WriteLine("Could not insert into table: {0}", e.Message);
                    }


                }
            }
        }
    }
}

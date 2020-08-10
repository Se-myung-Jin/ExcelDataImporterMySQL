using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace MySQL
{
    public class MySql
    {
        MySqlConnection conn = null;

        public MySql(string ConnectionString)
        {
            try
            {
                MySqlConnection conn = new MySqlConnection(ConnectionString);
                conn.Open();
                this.conn = conn;
            }
            catch (Exception e)
            {
                Console.WriteLine($"DB connect failed : {e.Message}");
                return;
            }
        }

        public bool ExecuteQuery(string query, out MySqlDataReader results)
        {
            MySqlCommand cmd = null;
            results = null;

            try
            {
                cmd = new MySqlCommand(query, conn);
                results = cmd.ExecuteReader();
            }
            catch (MySqlException e)
            {
                Console.WriteLine($"query: {cmd.CommandText}");
                Console.WriteLine($"Msg: {e.Message}");
                return false;
            }

            return true;
        }

        public bool ExecuteQuery(string query, ref List<Dictionary<String, object>> Rows)
        {
            if (ExecuteQuery(query, out var results))
            {
                if (results != null && results.IsClosed == false)
                {
                    while (results.Read())
                    {
                        var row = new Dictionary<String, object>();

                        for (int i = 0; i < results.FieldCount; i++)
                        {
                            string key = results.GetName(i);
                            object value = results[i];

                            row.Add(results.GetName(i), value);
                        }
                        Rows.Add(row);
                    }
                    results.Close();
                    return true;
                }
            }
            return false;
        }

        public bool ExecuteQuery(string query)
        {
            var cmd = new MySqlCommand(query);
            return ExecuteQuery(cmd);
        }

        public bool ExecuteQuery(MySqlCommand cmd)
        {
            try
            {
                cmd.Connection = conn;
                cmd.ExecuteNonQuery();
            }
            catch (MySqlException e)
            {
                Console.WriteLine($"query: {cmd.CommandText}");
                Console.WriteLine($"Msg: {e.Message}");
                return false;
            }

            return true;
        }

        public bool ImportFrom(ExcelImportBase import)
        {
            string columnNames = import.ColumnNames.Aggregate(new StringBuilder(),
                (current, next) => current.Append(current.Length == 0 ? "" : ",").Append(next)).ToString();

            StringBuilder query = null;

            using (var trx = conn.BeginTransaction())
            {
                int count = 0;
                try
                {
                    var delCmd = new MySqlCommand($"DELETE FROM {import.FileName};", conn);
                    delCmd.ExecuteNonQuery();

                    count = 1;
                    import.ResetId();

                    MySqlCommand cmd = null;

                    foreach (var r in import.GetRows())
                    {
                        count++;
                        var result = import.GetValues(r);

                        query = new StringBuilder();

                        query.Append($"INSERT INTO {import.TableName} ({columnNames}) VALUES (");

                        foreach (var res in result)
                        {
                            if (res == null)
                                continue;

                            string str = Convert.ToString(res);

                            if (res.GetType() == typeof(string))
                            {
                                query.Append("'");
                                query.Append(str);
                                query.Append("'");
                            }
                            else
                            {
                                query.Append(str);
                            }

                            if (res != result.LastOrDefault())
                                query.Append(", ");

                        }

                        query.Append(");");
                        cmd = new MySqlCommand(query.ToString(), conn);
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (MySqlException e)
                {
                    trx.Rollback();
                    Console.WriteLine($"QI: {import.qi}");
                    Console.WriteLine($"Row: {count}");
                    Console.WriteLine($"Msg: {e.Message}");
                    return false;
                }
                catch (Exception e)
                {
                    trx.Rollback();
                    Console.WriteLine($"QI: {import.qi}");
                    Console.WriteLine($"Row: {count}");
                    Console.WriteLine($"Msg: {e.Message}");
                    return false;
                }

                trx.Commit();
                conn.Close();
            }

            Console.WriteLine($"{import.TableName} Done");
            return true;
        }

    }
}

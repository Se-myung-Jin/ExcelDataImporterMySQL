using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using ImportLib;
using System.Collections.Concurrent;
using System.Configuration;

namespace ExcelDataImporterMySQL
{
    class Program
    {
        public static ConcurrentQueue<ExcelImportBase> excelSheetQ = new ConcurrentQueue<ExcelImportBase>();
        public static ConcurrentQueue<Action> loadSheetTaskQ = new ConcurrentQueue<Action>();
        public static ConcurrentQueue<string> errorMsgQ = new ConcurrentQueue<string>();
        public static ConcurrentQueue<Tuple<ExcelImportBase, List<object[]>, List<object[]>, List<object[]>>> changesQ = new ConcurrentQueue<Tuple<ExcelImportBase, List<object[]>, List<object[]>, List<object[]>>>();

        static Dictionary<string, MySql.eIdType> idTypeDic = new Dictionary<string, MySql.eIdType>();
        static Dictionary<string, int> idIdxDic = new Dictionary<string, int>();

        [STAThread]
        static int Main(string[] args)
        {
            String dirPathName;
            if (args.Length < 1)
            {
                FolderBrowserDialog dlg = new FolderBrowserDialog();

                if (dlg.ShowDialog() == DialogResult.OK && !string.IsNullOrWhiteSpace(dlg.SelectedPath))
                {
                    dirPathName = dlg.SelectedPath;
                }
                else
                {
                    MessageBox.Show("파일이 존재하는 폴더를 선택하세요.");
                    return -1;
                }
            }
            else
            {
                dirPathName = System.IO.Path.GetFullPath(args[0]);
            }

            if (!Directory.Exists(dirPathName))
            {
                MessageBox.Show("존재하는 폴더를 선택해 주세요.");
                return -1;
            }

            int retVal = LoadAllExcelSheet(dirPathName);

            while (true)
            {
                Console.WriteLine("0 : 종료, 1 : 테이블 검사 후 임포트");
                var input = Console.ReadLine();

                if (input == "0")
                    break;
                else if (input == "1")
                {
                    ExecuteImport(dirPathName);
                    break;
                }
            }

            Console.WriteLine("Press [Enter] to exit.");
            Console.ReadLine();

            return retVal;
        }

        #region Load all excel files
        public static int LoadAllExcelSheet(string dirPathName)
        {
            Task[] tasks = new Task[8];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    while (true)
                    {
                        if (!loadSheetTaskQ.TryDequeue(out var action))
                            return;

                        action.Invoke();
                    }
                });
            }

            foreach (var task in tasks)
                task.Wait();

            return 0;
        }

        public static void LoadExcelSheetAsync<T>(string dirPath, string sheetName)
        {
            var act = new Action(() =>
            {
                bool load = LoadExcelSheet<T>(dirPath, sheetName, out var import);

                if (load)
                    excelSheetQ.Enqueue(import);
            });

            loadSheetTaskQ.Enqueue(act);
        }

        public static bool LoadExcelSheet<T>(string dirPath, string sheetName, out ExcelImportBase import)
        {
            import = Activator.CreateInstance(typeof(T)) as ExcelImportBase;

            import.LoadSheetLoader(dirPath, sheetName);

            if (!import.Import())
            {
                Console.WriteLine($"Excel import failed, {import.TableName}");
                return false;
            }
            else
            {
                Console.WriteLine("{0}.xlsx 파일을 성공적으로 로딩했습니다.", sheetName);
            }
            return true;
        }
        #endregion

        public static int ExecuteImport(string dirPathName)
        {
            AppSettingsReader settingsReader = new AppSettingsReader();
            string connStr = settingsReader.GetValue("PostgreSQLConnectionString", typeof(string)) as string;

            Console.WriteLine(connStr);

            MySql db = new MySql(connStr);

            Task[] tasks = new Task[8];
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    MySql taskdb = new MySql(connStr);

                    while (true)
                    {
                        if (!excelSheetQ.TryDequeue(out var import))
                            return;

                        TryCopyToDB(taskdb, import);
                    }
                });
            }

            foreach (var task in tasks)
                task.Wait();

            if (PrintErrorMsg())
                return 1;

            while (true)
            {
                if (!changesQ.TryDequeue(out var changes))
                    break;

                ApplyChanges(db, changes);
            }
            return 0;
        }

        public static bool PrintErrorMsg()
        {
            bool error = errorMsgQ.Count > 0;
            Console.ForegroundColor = ConsoleColor.Red;

            while (true)
            {
                if (!errorMsgQ.TryDequeue(out var errorMsg))
                    break;

                Console.WriteLine(errorMsg);
            }
            Console.ResetColor();

            return error;
        }

        public static bool TryCopyToDB(MySql db, ExcelImportBase import)
        {
            var dbRows = new List<Dictionary<String, object>>();

            if (!db.ExecuteQuery($"select * from {import.TableName};", ref dbRows))
            {
                Console.WriteLine($"DB query failed, {import.TableName}");
                return false;
            }

            var dbValues = new List<object[]>();

            if (!GetAllValues(dbRows, import.ColumnNames, ref dbValues))
            {
                Console.WriteLine($"Get db query failed, {import.TableName}");
                return false;
            }

            var tableValues = new List<object[]>();

            import.ResetId();
            foreach (var row in import.GetRows())
            {
                try
                {
                    var tableValue = import.GetValues(row);
                    tableValues.Add(tableValue);
                }
                catch (Exception e)
                {
                    errorMsgQ.Enqueue($"{import.FileName} 테이블에서 예외가 발생하였습니다.");
                    return false;
                }
            }

            var addedTable = new List<object[]>();
            var missingTable = new List<object[]>();

            if (!GetAllChanges(ref tableValues, ref dbValues, ref addedTable, ref missingTable))
            {
                Console.WriteLine($"{import.TableName} 테이블 데이터의 바뀐 내용이 없습니다.");
                return true;
            }
            else
            {
                var modifiedTable = new List<object[]>();
                GetModifiedTable(ref addedTable, ref modifiedTable, ref missingTable, import.FileName);
                var changes = new Tuple<ExcelImportBase, List<object[]>, List<object[]>, List<object[]>>(import, addedTable, modifiedTable, missingTable);
                changesQ.Enqueue(changes);
                Console.WriteLine($"{import.TableName} 테이블의 수정 사항이 생겼습니다.");
                return true;
            }
        }

        public static bool GetAllValues(List<Dictionary<String, object>> Rows, string[] ColumnNames, ref List<object[]> allValues)
        {
            for (int i = 0; i < Rows.Count; i++)
            {
                var row = Rows[i];
                object[] values = new object[ColumnNames.Length];

                for (int j = 0; j < ColumnNames.Length; j++)
                {
                    var columnName = ColumnNames[j];
                    if (row.TryGetValue(columnName, out var value))
                    {
                        values[j] = value;
                    }
                    else
                    {
                        return false;
                    }
                }
                allValues.Add(values);
            }

            return true;
        }

        public static bool GetAllChanges(ref List<object[]> tableValues, ref List<object[]> dbValues, ref List<object[]> addedTable, ref List<object[]> missingTable)
        {
            tableValues = tableValues.OrderBy((val) => val?[0]).ToList();
            dbValues = dbValues.OrderBy((val) => val?[0]).ToList();

            bool isChange = false;

            for (int i = 0; i < tableValues.Count; i++)
            {
                object[] tableValue = tableValues[i];

                if (tableValue == null)
                    continue;

                bool find = false;
                int remove = -1;

                int tableNumberValue = -1;

                try
                {
                    tableNumberValue = Convert.ToInt32(tableValue[0]);
                }
                catch (Exception e)
                {

                }

                for (int j = 0; j < dbValues.Count; j++)
                {
                    object[] dbValue = dbValues[j];

                    if (dbValue == null)
                        continue;

                    int DBNumberValue = -1;

                    try
                    {
                        DBNumberValue = Convert.ToInt32(dbValue[0]);
                    }
                    catch (Exception e)
                    {

                    }

                    if (DBNumberValue > tableNumberValue)
                    {
                        find = false;

                        break;
                    }
                    else if (DBNumberValue != tableNumberValue)
                    {
                        remove = j;
                    }

                    for (int k = 0; k < dbValue.Length; k++)
                    {
                        var tableValueString = tableValue[k]?.ToString() ?? "null";
                        var dbValueString = dbValue[k]?.ToString() ?? "null";

                        dbValueString = dbValueString == "" ? "null" : dbValueString;

                        if (tableValueString != dbValueString)
                        {
                            find = false;
                            break;
                        }

                        if (k == dbValue.Length - 1)
                        {
                            find = true;
                        }
                    }

                    if (find)
                    {
                        dbValues.RemoveAt(j);
                        break;
                    }
                }

                if (!find)
                {
                    addedTable.Add(tableValue);

                    isChange = true;
                }

                if (remove != -1)
                {
                    for (int k = 0; k < remove; k++)
                    {
                        var removeValue = dbValues[0];
                        missingTable.Add(removeValue);
                        dbValues.RemoveAt(0);
                    }
                }
            }

            foreach (var dbValue in dbValues)
            {
                missingTable.Add(dbValue);
                isChange = true;
            }

            return isChange;
        }

        #region Modifiy Table
        public static void GetModifiedTable(ref List<object[]> addedTable, ref List<object[]> modifiedTable, ref List<object[]> missingTable, string fileName)
        {
            var type = MySql.eIdType.Default;
            int idIdx = 0;

            idTypeDic.TryGetValue(fileName, out type);
            idIdxDic.TryGetValue(fileName, out idIdx);

            try
            {
                switch (type)
                {
                    case MySql.eIdType.Default:
                        GetModifiedTableDefault(ref addedTable, ref modifiedTable, ref missingTable, idIdx);
                        break;
                    case MySql.eIdType.Generate:
                        GetModifiedTableGenerate(ref addedTable, ref modifiedTable, ref missingTable);
                        break;
                    case MySql.eIdType.Overlap:
                        GetModifiedTableOverlap(ref addedTable, ref modifiedTable, ref missingTable);
                        break;
                }
            }
            catch (Exception e)
            {
                errorMsgQ.Enqueue($"{fileName} 테이블에서 예외가 발생하였습니다.");

            }
        }

        private static void GetModifiedTableDefault(ref List<object[]> addedTable, ref List<object[]> modifiedTable, ref List<object[]> missingTable, int idIdx)
        {
            var addedDic = addedTable.ToDictionary(val => val[idIdx].ToString());

            foreach (var missing in missingTable)
            {
                var key = missing[idIdx].ToString();

                if (addedDic.TryGetValue(key, out var objectArr))
                {
                    modifiedTable.Add(objectArr);
                }
            }

            foreach (var modi in modifiedTable)
            {
                missingTable.RemoveAll(val => val[idIdx].ToString() == modi[idIdx].ToString());
                addedTable.Remove(modi);
            }
        }

        private static void GetModifiedTableGenerate(ref List<object[]> addedTable, ref List<object[]> modifiedTable, ref List<object[]> missingTable)
        {
            var addedList = new List<object[]>();
            var missingList = new List<object[]>();

            addedTable = addedTable.OrderBy(val => val[1]).ToList();
            missingTable = missingTable.OrderBy(val => val[1]).ToList();

            foreach (var added in addedTable)
            {
                bool find = true;

                int count = 0;

                int addedNumberValue = -1;

                try
                {
                    addedNumberValue = Convert.ToInt32(added[1]);
                }
                catch (Exception e)
                {

                }

                foreach (var missing in missingTable)
                {
                    find = true;

                    count++;

                    int missingNumberValue = -1;

                    try
                    {
                        missingNumberValue = Convert.ToInt32(missing[1]);
                    }
                    catch (Exception e)
                    {

                    }

                    if (missingNumberValue > addedNumberValue)
                    {
                        find = false;
                        break;
                    }

                    for (int k = 1; k < added.Length; k++)
                    {
                        if (added[k].ToString() != missing[k].ToString())
                        {
                            find = false;
                            break;
                        }
                    }

                    if (find)
                    {
                        addedList.Add(added);
                        missingList.Add(missing);
                        missingTable.RemoveAt(count - 1);
                        break;
                    }
                }
            }

            foreach (var added in addedList)
            {
                addedTable.Remove(added);
            }

            foreach (var missing in missingList)
            {
                missingTable.Remove(missing);
            }

            addedList.Clear();
            missingList.Clear();

            GetModifiedTableOverlap(ref addedTable, ref modifiedTable, ref missingTable);
        }

        private static void GetModifiedTableOverlap(ref List<object[]> addedTable, ref List<object[]> modifiedTable, ref List<object[]> missingTable)
        {
            var removeAddedList = new List<object[]>();

            foreach (var added in addedTable)
            {
                foreach (var missing in missingTable)
                {
                    int count = 0;

                    for (int k = 1; k < added.Length; k++)
                    {
                        if (added[k].ToString() == missing[k].ToString())
                        {
                            count++;
                        }
                    }

                    if (count > added.Length * 0.5f)
                    {
                        modifiedTable.Add(added);
                        removeAddedList.Add(added);
                        missingTable.Remove(missing);
                        break;
                    }
                }
            }

            foreach (var removed in removeAddedList)
            {
                addedTable.Remove(removed);
            }
        }
        #endregion

        #region apply changes
        private static bool ApplyChanges(MySql db, Tuple<ExcelImportBase, List<object[]>, List<object[]>, List<object[]>> changes)
        {
            var import = changes.Item1;
            var addedTable = changes.Item2;
            var modifiedTable = changes.Item3;
            var missingTable = changes.Item4;

            int idIdx = 0;
            idIdxDic.TryGetValue(import.FileName, out idIdx);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{import.TableName} 테이블에 추가된 항목 수 : {addedTable.Count}");
            foreach (object[] added in addedTable)
            {
                Console.WriteLine($"{import.TableName} 테이블에 추가된 항목의 id : {added[idIdx]}");
            }

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"{import.TableName} 테이블에 수정된 항목 수 : {modifiedTable.Count}");
            foreach (object[] modi in modifiedTable)
            {
                Console.WriteLine($"{import.TableName} 테이블에 수정된 항목의 id : {modi[idIdx]}");
            }

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{import.TableName} 테이블에 누락된 항목 수 : {missingTable.Count}");
            foreach (object[] missing in missingTable)
            {
                Console.WriteLine($"{import.TableName} 테이블에 누락된 항목의 id : {missing[idIdx]}");
            }

            Console.ResetColor();

            if (!DecideApply(import.TableName))
                return false;

            if (!db.ImportFrom(import))
            {
                Console.WriteLine($"DB Insert Fail!!, {import.TableName}");
                return false;
            }

            return true;
        }

        private static bool DecideApply(string tableName)
        {
            while (true)
            {
                Console.WriteLine($"{tableName} 테이블에 변경된 내용을 적용하시겠습니까? [ y / n ]");

                var input = Console.ReadLine().ToLower();

                if (input == "y")
                    return true;

                else if (input == "n")
                    return false;

                else
                    continue;
            }
        }
        #endregion
    }
}

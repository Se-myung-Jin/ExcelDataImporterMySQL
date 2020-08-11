using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RowsRetType = System.Collections.Generic.List<System.Collections.Generic.Dictionary<string, string>>;
using RowRetType = System.Collections.Generic.Dictionary<string, string>;
using System.ComponentModel;

namespace ImportLib
{
    public enum ExcelName
    {
        Employees,
    }

    public abstract class ExcelImportBase
    {
        public string FileName { get; protected set; }
        public string TableName { get; protected set; }
        public string[] ColumnNames { get; protected set; }
        protected RowsRetType Rows = new RowsRetType();
        public IEnumerable<RowRetType> GetRows() => Rows;
        public ExcelName eName { get; protected set; }
        string[] replaceStrings = { "{ }", "{}" };
        protected ExcelSheetLoader Loader;

        protected int id = 0;

        protected ExcelImportBase(ExcelName eName)
        {
            this.eName = eName;
        }

        public void SetTableInfo(string tableName, string[] columnNames)
        {
            this.TableName = tableName;
            this.ColumnNames = columnNames;
        }

        public abstract object[] GetValues(Dictionary<string, string> Row);

        public T GetValue<T>(RowRetType row, string key, bool check_empty = true)
        {
            string value;

            if (row.TryGetValue(key, out value) == false)
            {
                throw new System.Exception("Col[" + key + "] name is invalid");
            }
            if (value.ToString() == "")
            {
                if (check_empty == true)
                {
                    throw new System.Exception("Col[" + key + "] value is empty");
                }
                else
                {
                    return default(T);
                }
            }
            if (replaceStrings.Contains(value))
                return default(T);

            try
            {
                return (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFromString(value);
            }
            catch (Exception)
            {
                throw new Exception($"Col[{key}] Convert Fail, value:{value} to {typeof(T).Name}");
            }
        }
        public T GetValue<T>(Dictionary<String, T> dic, RowRetType row, string key)
        {
            string value;
            if (row.TryGetValue(key, out value) == false)
            {
                throw new System.Exception("Col[" + key + "] name is invalid");
            }
            if (value.ToString() == "")
            {
                throw new System.Exception("Col[" + key + "] value is empty");
            }
            value = value.ToLower().Trim();
            T dic_value;
            if (dic.TryGetValue(value, out dic_value) == false)
            {
                throw new System.Exception("In Col[" + key + "] this keyword[" + value + "] is invalid.");
            }
            return dic_value;
        }

        private void LoadSheetLoader(string filePath)
        {
            Loader = new ExcelSheetLoader(filePath, EnumColumnFormat.NONE);
        }

        public void LoadSheetLoader(string folderPath, string sheetName)
        {
            FileName = sheetName;
            string filePath = $"{folderPath}\\{sheetName}.xlsx";
            LoadSheetLoader(filePath);
        }

        public bool Import()
        {
            if (!Loader.LoadSheet(ref Rows))
            {
                Console.WriteLine("{0}", Loader.ErrMsg);
                Console.WriteLine("엑셀 파일 불러오기 실패했습니다. [Enter] 키를 눌러주세요.");
                Console.ReadKey();
                return false;
            }

            return true;
        }

        public virtual void ResetId()
        {
            id = 0;
        }
    }
}

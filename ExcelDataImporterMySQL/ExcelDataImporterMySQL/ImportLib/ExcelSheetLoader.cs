using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportLib
{
    public enum EnumColumnFormat
    {
        NONE,
        LOWER,
        UPPER,
    }

    public class ExcelSheetLoader : IDisposable
    {
        String FileName;
        public String ErrMsg;
        private EnumColumnFormat ColumnFormat;

        public ExcelSheetLoader(String ExcelFileName, EnumColumnFormat Format)
        {
            try
            {
                FileName = ExcelFileName;
                ColumnFormat = Format;
            }
            catch (Exception e)
            {
                ErrMsg = e.Message;
            }
        }

        ~ExcelSheetLoader()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {

            }
        }

        public bool LoadSheet(ref List<Dictionary<String, String>> rows)
        {
            Dictionary<int, string> columns = new Dictionary<int, string>();

            try
            {
                using (var package = new OfficeOpenXml.ExcelPackage(new System.IO.FileInfo(FileName)))
                {
                    OfficeOpenXml.ExcelPackage.LicenseContext = OfficeOpenXml.LicenseContext.NonCommercial;

                    var workSheet = package.Workbook.Worksheets.FirstOrDefault();

                    if (workSheet == null)
                    {
                        ErrMsg = $"{FileName} 파일이 없거나 데이터가 존재하지 않습니다.";
                        return false;
                    }

                    for (int r = 1; r <= workSheet.Dimension.End.Row; r++)
                    {
                        Dictionary<string, string> row = null;

                        if (r != 1)
                        {
                            row = new Dictionary<string, string>();
                            rows.Add(row);
                        }

                        for (int c = 1; c <= workSheet.Dimension.End.Column; c++)
                        {
                            string cellVal = workSheet.Cells[r, c].Value?.ToString() ?? "";

                            if (r == 1)
                            {
                                if (cellVal.Length == 0)
                                    continue;

                                if (ColumnFormat == EnumColumnFormat.LOWER)
                                    columns.Add(c, cellVal.ToLower());
                                else if (ColumnFormat == EnumColumnFormat.UPPER)
                                    columns.Add(c, cellVal.ToUpper());
                                else
                                    columns.Add(c, cellVal.Trim());
                            }
                            else
                            {
                                if (columns.TryGetValue(c, out var columnString))
                                {
                                    if (row.ContainsKey(columnString))
                                    {
                                        Console.WriteLine($"{columnString}키가 이미 존재합니다.");
                                    }
                                    else
                                    {
                                        row.Add(columnString, cellVal.ToString());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                ErrMsg = e.StackTrace;
                return false;
            }
            return true;
        }
    }
}

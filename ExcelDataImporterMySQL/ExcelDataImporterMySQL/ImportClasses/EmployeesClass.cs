using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ImportLib;
using RowRetType = System.Collections.Generic.Dictionary<string, string>;

namespace ImportClasses
{
    class EmployeesClass : ExcelImportBase
    {
        public EmployeesClass() : base(ExcelName.Employees)
        {
            SetTableInfo("employees", new String[] { "empid", "emplastname", "empfirstname" });
        }

        public override object[] GetValues(RowRetType Row)
        {
            return new object[]
            {
                GetValue<int>(Row, "empid"),
                GetValue<string>(Row, "emplastname"),
                GetValue<string>(Row, "empfirstname"),
            };
        }
    }
}

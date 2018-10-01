using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlPocoHelpers
{
    public static class Display
    {

        public static string ArgsAsSql(string sql, List<Object> parameters)
        {
            var sb = new StringBuilder();
            sql = sql.Split('@')[0];
            sb.Append(sql);
            sb.Append(" ");
            var first = true;
            foreach (var param in parameters)
            {
                object pValue = null;
                var name = "";

                var p = param as ObjectParameter;
                
                if (p != null)
                {
                    pValue = p.Value;
                    name = p.Name;
                }
                var p2 = param as DbParameter;
                if (p2 != null)
                {
                    pValue = p2.Value;
                    name = p2.ParameterName;
                }

                name = name.Replace("@", "");

                var type = pValue?.GetType() ?? typeof(string);
                if (!first)
                {
                    sb.Append(", ");
                }
                if (pValue == null || pValue == DBNull.Value)
                    sb.AppendFormat("@{0} = NULL", name);
                else if (type == typeof(DateTime))
                    sb.AppendFormat("@{0} ='{1}'", name, ((DateTime)pValue).ToString("yyyy-MM-dd HH:mm:ss.fff"));
                else if (type == typeof(bool))
                    sb.AppendFormat("@{0} = {1}", name, (bool)pValue ? 1 : 0);
                else if (type == typeof(int))
                    sb.AppendFormat("@{0} = {1}", name, pValue);
                else if (type == typeof(long))
                    sb.AppendFormat("@{0} = {1}", name, pValue);
                else if (type == typeof(float))
                    sb.AppendFormat("@{0} = {1}", name, pValue);
                else if (type == typeof(double))
                    sb.AppendFormat("@{0} = {1}", name, pValue);
                else
                    sb.AppendFormat("@{0} = '{1}'", name, pValue?.ToString());

                first = false;

            }
            

            return sb.ToString();
        }

        public static string ArgsAsSql(this Database db, string sql, List<Object> parameters)
        {
            return ArgsAsSql(sql, parameters);
        }
        public static void DebugWrite(this Database db, string sql, List<Object> parameters)
        {
            var st = ArgsAsSql(sql, parameters);
            //LastDbCall = st;
            db.Log(st);
        }

        public static string ArgsAsSql(this SqlConnection db, string sql, List<Object> parameters)
        {
            return ArgsAsSql(sql, parameters);
        }
        public static void DebugWrite(this SqlConnection db, string sql, List<Object> parameters)
        {
            var st = ArgsAsSql(sql, parameters);
            //LastDbCall = st;
            Debug.WriteLine(st);
        }


    }
}

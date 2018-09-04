using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PocoPropertyData;

namespace SqlPocoHelpers
{
    public static class Odbc
    {


        public static OdbcParameter Param(string name, object value)
        {
            return new OdbcParameter(name, (value ?? DBNull.Value));
        }


        public static bool DoesFieldExist(OdbcConnection conn, string tableName, string fieldName)
        {
            string SQL = "SELECT top 1 [" + fieldName + "] FROM [" + tableName + "]";
            try
            {
                var i = conn.QueryScaler(SQL, null);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }


        

        public static Object QueryScaler(this OdbcConnection db, string sql, List<OdbcParameter> parameters)
        {
            try
            {

                parameters = parameters ?? new List<OdbcParameter>();
                var conn = db;
                {
                    var cmd = new OdbcCommand(sql, conn);
                    cmd.CommandTimeout = (db.ConnectionTimeout != 0 ? db.ConnectionTimeout : 20);

                    foreach (var param in parameters)
                    {
                        cmd.Parameters.Add(param);
                    }
                    return cmd.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                var tstr = sql + "";
                foreach (var param in parameters)
                {
                    var p2 = param;
                    if (p2 != null)
                    {
                        tstr = tstr + " " + p2.ParameterName + " = " + (p2.Value == null ? "null" : "'" + p2.Value + "'");
                    }
                }
                throw new Exception("Error: " + ex.Message + " on db call:" + tstr, ex);
            }
        }
        

            public static DataSet QueryDataSet(this OdbcConnection db, string sql, List<OdbcParameter> parameters)
        {
            try
            {

                parameters = parameters ?? new List<OdbcParameter>();
                var ds = new DataSet();
                var conn = db;
                {
                    var cmd = new OdbcCommand(sql, conn);
                    cmd.CommandTimeout = (db.ConnectionTimeout != 0 ? db.ConnectionTimeout : 20);

                    foreach (var param in parameters)
                    {
                        cmd.Parameters.Add(param);
                    }

                    var da = new OdbcDataAdapter(cmd);


                    da.Fill(ds);
                }
                return ds;
            }
            catch (Exception ex)
            {
                var tstr = sql + "";
                foreach (var param in parameters)
                {
                    
                    var p2 = param;
                    if (p2 != null)
                    {
                        tstr = tstr + " " + p2.ParameterName + " = " + (p2.Value == null ? "null" : "'" + p2.Value + "'");
                    }
                }
                throw new Exception("Error: " + ex.Message + " on db call:" + tstr, ex);
            }
        }

        public static DataTable QueryTable(this OdbcConnection db, string sql, List<OdbcParameter> parameters)
        {
            return db.QueryDataSet(sql, parameters).Tables[0];
        }
        


        //public static void ExecuteCommandExtended(this OdbcConnection db, string sql,
        //    params OdbcParameter[] parameters)
        //{
        //    db.ExecuteCommandExtended(sql, (from p in parameters select p as OdbcParameter).ToList());
        //}
        
        

        public static void ExecuteCommandExtended(this OdbcConnection db, string sql,
            List<OdbcParameter> parameters = null)
        {
            db.QueryDataSet(sql, parameters);
        }
        
        public static IQueryable<TTt> QueryExtended<TTt>(this OdbcConnection db, string sql,
            List<OdbcParameter> parameters = null, Dictionary<string, string> mappings = null) where TTt : class
        {
            parameters = parameters ?? new List<OdbcParameter>();
            mappings = mappings ?? new Dictionary<string, string>();
            var st = DateTime.Now;
            var tbl = db.QueryTable(sql, parameters);
            var st2 = DateTime.Now;
            var q = tbl.ToList<TTt>(columnMapping: mappings).AsQueryable();
            return q;
        }

        //public static IQueryable<TTt> QueryExtended<TTt>(this OdbcConnection db, string sql,
        //    List<Object> parameters = null, Dictionary<string, string> mappings = null) where TTt : class
        //{
        //    parameters = parameters ?? new List<Object>();
        //    mappings = mappings ?? new Dictionary<string, string>();
        //    var st = DateTime.Now;
        //    var tbl = db.QueryTable(sql, (from p in parameters select (object)p).ToList());
        //    var st2 = DateTime.Now;
        //    var q = tbl.ToList<TTt>(columnMapping: mappings).AsQueryable();
        //    return q;
        //}

        public static Tuple<IQueryable<TT1>, IQueryable<TT2>> QueryExtended<TT1, TT2>(this OdbcConnection db, string sql, List<OdbcParameter> parameters = null, Dictionary<string, string> mappings1 = null, Dictionary<string, string> mappings2 = null)
            where TT1 : class
            where TT2 : class
        {
            mappings1 = mappings1 ?? new Dictionary<string, string>();
            mappings2 = mappings2 ?? new Dictionary<string, string>();
            var st = DateTime.Now;
            var ds = db.QueryDataSet(sql, parameters);
            var st2 = DateTime.Now;
            IQueryable<TT1> q1 = null;
            IQueryable<TT2> q2 = null;
            if (ds.Tables.Count >= 1)
            {
                q1 = ds.Tables[0].ToList<TT1>(columnMapping: mappings1).AsQueryable();
            }
            if (ds.Tables.Count >= 2)
            {
                q2 = ds.Tables[1].ToList<TT2>(columnMapping: mappings2).AsQueryable();
            }
            var q = new Tuple<IQueryable<TT1>, IQueryable<TT2>>(q1, q2);
            return q;
        }
        //public static Tuple<IQueryable<TT1>, IQueryable<TT2>> QueryExtended<TT1, TT2>(this OdbcConnection db, string sql, List<Object> parameters = null, Dictionary<string, string> mappings1 = null, Dictionary<string, string> mappings2 = null)
        //    where TT1 : class
        //    where TT2 : class
        //{
        //    mappings1 = mappings1 ?? new Dictionary<string, string>();
        //    mappings2 = mappings2 ?? new Dictionary<string, string>();
        //    var st = DateTime.Now;
        //    var ds = db.QueryDataSet(sql, parameters);
        //    var st2 = DateTime.Now;
        //    IQueryable<TT1> q1 = null;
        //    IQueryable<TT2> q2 = null;
        //    if (ds.Tables.Count >= 1)
        //    {
        //        q1 = ds.Tables[0].ToList<TT1>(columnMapping: mappings1).AsQueryable();
        //    }
        //    if (ds.Tables.Count >= 2)
        //    {
        //        q2 = ds.Tables[1].ToList<TT2>(columnMapping: mappings2).AsQueryable();
        //    }
        //    var q = new Tuple<IQueryable<TT1>, IQueryable<TT2>>(q1, q2);
        //    return q;
        //}



        public static IQueryable<TTt> QueryExtended<TTt>(this OdbcConnection db, string sql, params OdbcParameter[] parameters) where TTt : class
        {
            var map = new Dictionary<string, string>();
            return db.QueryExtended<TTt>(sql, (from p in parameters select p).ToList(), map);
        }
        
        

    }
}

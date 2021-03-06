﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.SqlClient;
using System.Linq;
using PocoPropertyData;

namespace SqlPocoHelpers
{
    public static class Sql
    {
        public static SqlParameter Param(string name, object value)
        {
            return new SqlParameter(name, (value ?? DBNull.Value));
        }

        public static DataSet SqlQueryDataSet(this  Database db, string sql, List<Object> parameters)
        {
            try
            {

            if (parameters == null)
            {
                parameters = new List<object>();
            }
            var ds = new DataSet();
            var conn = (SqlConnection)db.Connection;
            {
                db.Log(sql);
                var cmd = new SqlCommand(sql, conn);
                cmd.CommandTimeout = (db.CommandTimeout.HasValue ? db.CommandTimeout.Value : 20);

                foreach (var param in parameters)
                {
                    cmd.Parameters.Add(param);
                    var p = param as ObjectParameter;
                    if (p != null)
                    {
                        db.Log("-- " + p.Name + ": '" + p.Value + "'");
                    }
                    var p2 = param as DbParameter;
                    if (p2 != null)
                    {
                        db.Log("-- " + p2.ParameterName + ": '" + p2.Value + "'");
                    }
                }

                var da = new SqlDataAdapter(cmd);


                var st = DateTime.Now;
                db.Log("-- Executing at " + DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString());
                da.Fill(ds);
                db.Log("-- Loaded in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            }
            return ds;
            }
            catch (Exception ex)
            {
                var tstr = sql + "";
                foreach (var param in parameters)
                {
                    
                    var p = param as ObjectParameter;
                    if (p != null)
                    {
                        tstr = tstr + " " + p.Name + " = " + (p.Value == null ? "null" : "'" + p.Value + "'");
                    }
                    var p2 = param as DbParameter;
                    if (p2 != null)
                    {
                        tstr = tstr + " " + p2.ParameterName + " = " + (p2.Value == null ? "null" : "'" + p2.Value + "'");
                    }
                }
                throw new Exception("Error: " + ex.Message +" on db call:" + tstr, ex);
            }
        }


        public static DataTable SqlQueryTable(this Database db, string sql, List<Object> parameters)
        {
            return db.SqlQueryDataSet(sql, parameters).Tables[0];
        }



        public static void ExecuteSqlCommandExtended(this Database db, string sql,
            params SqlParameter[] parameters)
        {
            db.ExecuteSqlCommandExtended(sql, (from p in parameters select p as Object).ToList());
        }

        public static void ExecuteSqlCommandExtended(this DbContext db, string sql,
            params SqlParameter[] parameters) 
        {
             db.Database.ExecuteSqlCommandExtended(sql, (from p in parameters select p as Object).ToList());
        }

        public static void ExecuteSqlCommandExtended(this DbContext db, string sql,
            List<SqlParameter> parameters = null)
        {

             db.Database.ExecuteSqlCommandExtended(sql, (from p in parameters select p as Object).ToList());
        }

        public static void ExecuteSqlCommandExtended(this Database db, string sql, List<Object> parameters = null)
        {
            var st = DateTime.Now;
            db.SqlQueryDataSet(sql, parameters);
            db.Log("-- Completed in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
        }
        public static IQueryable<TTt> SqlQueryExtended<TTt>(this DbContext db, string sql,
            List<SqlParameter> parameters = null, Dictionary<string, string> mappings = null) where TTt : class
        {
            if (mappings == null)
            {
                mappings = new Dictionary<string, string>();
            }
            return db.Database.SqlQueryExtended<TTt>(sql, (from p in parameters select p as Object).ToList(), mappings);
        }


        public static IQueryable<TTt> SqlQueryExtended<TTt>(this Database db, string sql, List<Object> parameters = null, Dictionary<string, string> mappings = null)
            where TTt : class
        {
            if (mappings == null)
            {
                mappings = new Dictionary<string, string>();
            }
            var st = DateTime.Now;
            var tbl = db.SqlQueryTable(sql, parameters);
            var st2 = DateTime.Now;
            var q = tbl.ToList<TTt>(columnMapping:mappings).AsQueryable();
            db.Log("-- Mapping in " + DateTime.Now.Subtract(st2).TotalMilliseconds + " ms");
            db.Log("-- Completed in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            return q;
        }


        public static Tuple<IQueryable<TT1>, IQueryable<TT2>> SqlQueryExtended<TT1, TT2>(this Database db,
            string sql, List<Object> parameters = null, Dictionary<string, string> mappings1 = null, Dictionary<string, string> mappings2 = null)
            where TT1 : class 
            where TT2 : class
        {
            mappings1 = mappings1 ?? new Dictionary<string, string>();
            mappings2 = mappings2 ?? new Dictionary<string, string>();
            var st = DateTime.Now;
            var ds = db.SqlQueryDataSet(sql, parameters);
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
            db.Log("-- Mapping in " + DateTime.Now.Subtract(st2).TotalMilliseconds + " ms");
            db.Log("-- Completed in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            return q;
        }
        public static Tuple<IQueryable<TT1>, IQueryable<TT2>, IQueryable<TT3>> SqlQueryExtended<TT1, TT2, TT3>(this Database db, string sql, List<Object> parameters = null, Dictionary<string, string> mappings1 = null, Dictionary<string, string> mappings2 = null, Dictionary<string, string> mappings3 = null)
            where TT1 : class
            where TT2 : class
            where TT3 : class
        {
            mappings1 = mappings1 ?? new Dictionary<string, string>();
            mappings2 = mappings2 ?? new Dictionary<string, string>();
            mappings3 = mappings3 ?? new Dictionary<string, string>();
            var st = DateTime.Now;
            var ds = db.SqlQueryDataSet(sql, parameters);
            var st2 = DateTime.Now;
            IQueryable<TT1> q1 = null;
            IQueryable<TT2> q2 = null;
            IQueryable<TT3> q3 = null;
            if (ds.Tables.Count >= 1)
            {
                q1 = ds.Tables[0].ToList<TT1>(columnMapping: mappings1).AsQueryable();
            }
            if (ds.Tables.Count >= 2)
            {
                q2 = ds.Tables[1].ToList<TT2>(columnMapping: mappings2).AsQueryable();
            }
            if (ds.Tables.Count >= 3)
            {
                q3 = ds.Tables[2].ToList<TT3>(columnMapping: mappings3).AsQueryable();
            }
            var q = new Tuple<IQueryable<TT1>, IQueryable<TT2>, IQueryable<TT3>>(q1, q2, q3);
            db.Log("-- Mapping in " + DateTime.Now.Subtract(st2).TotalMilliseconds + " ms");
            db.Log("-- Completed in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            return q;
        }




        [Obsolete("Use version with parameter list", true)]
        public static IQueryable<TTt> SqlQueryExtended<TTt>(this DbContext db, string sql,
            params SqlParameter[] parameters) where TTt : class
        {
            return db.Database.SqlQueryExtended<TTt>(sql, (from p in parameters select p as Object).ToList());
        }
        [Obsolete("Use version with parameter list then mappings. (sql, parameter, mappings)", true)]
        public static IQueryable<TTt> SqlQueryExtended<TTt>(this DbContext db, Dictionary<string, string> mappings, string sql,
            params SqlParameter[] parameters) where TTt : class
        {
            return db.Database.SqlQueryExtended<TTt>(sql, parameters, mappings);
        }
        [Obsolete("Use version with parameter list", true)]
        public static IQueryable<TTt> SqlQueryExtended<TTt>(this Database db, string sql, params object[] parameters) where TTt : class
        {
            var map = new Dictionary<string, string>();
            return db.SqlQueryExtended<TTt>(sql, (from p in parameters select p).ToList(), map);
        }
        [Obsolete("Use version with parameter list then mappings. (sql, parameter, mappings)", true)]
        public static IQueryable<TTt> SqlQueryExtended<TTt>(this Database db, Dictionary<string, string> mappings,
            string sql, params object[] parameters)
            where TTt : class
        {
            return db.SqlQueryExtended<TTt>(sql, (from p in parameters select p).ToList(), mappings);
        }
    }
}

// 日本語プログラミング言語「プロデル」データベースプラグイン
// Copyright(C) 2007-2024 utopiat.net https://github.com/utopiat-ire/
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Mono.Data.Sqlite;
using System.Data.Common;

namespace Produire.Data.Sqlite
{
	[種類(DocUrl = "/data/db/dbsqlite.htm")]
	public class SQLiteデータベース : データベースエンジン
	{
		#region フィールド

		string connectionString;
		SqliteConnectionStringBuilder constrBuilder = new SqliteConnectionStringBuilder();

		SqliteConnection connection = new SqliteConnection();
		SqliteTransaction transaction;

		#endregion

		public SQLiteデータベース()
		{
		}

		#region 手順

		/// <summary>データベースサーバへ接続します</summary>
		[自分へ]
		public override void 接続([で, 省略]string 接続文字列)
		{
			if (string.IsNullOrEmpty(接続文字列))
			{
				if (!string.IsNullOrEmpty(connectionString))
				{
					connection.ConnectionString = connectionString;
				}
				else
				{
					connection.ConnectionString = constrBuilder.ConnectionString;
				}
			}
			else
			{
				connection.ConnectionString = 接続文字列;
			}

			try
			{
				connection.Open();
			}
			catch (Exception ex)
			{
				throw new ProduireException(ex);
			}
		}

		/// <summary>トランザクション処理を開始します</summary>
		[自分で, 手順名("トランザクションを", "開始")]
		public override トランザクション トランザクションを開始()
		{
			if (connection.State == ConnectionState.Open)
			{
				transaction = connection.BeginTransaction() as SqliteTransaction;
				return new トランザクション(transaction);
			}
			else
			{
				throw new ProduireException("データベースへ接続されていません。");
			}
		}
		/// <summary>トランザクション処理をコミットして処理を完了させます</summary>
		[自分で, 手順名("トランザクションを", "完了")]
		public override void トランザクションを完了()
		{
			if (transaction != null)
			{
				transaction.Commit();
			}
			else
			{
				throw new ProduireException("トランザクションが開始されていません。");
			}
		}
		/// <summary>トランザクション処理をロールバックして処理を取り消します</summary>
		[自分で, 手順名("トランザクションを", "取り消す")]
		public override void トランザクションを取り消す()
		{
			if (transaction != null)
			{
				transaction.Rollback();
			}
			else
			{
				throw new ProduireException("トランザクションが開始されていません。");
			}
		}
		#endregion

		#region 設定項目

		public string データソース
		{
			get { return constrBuilder.DataSource; }
			set { 接続文字列 = "URI=file:" + value; }
		}
		public string 接続文字列
		{
			get { return constrBuilder.ConnectionString; }
			set
			{
				connectionString = value;
				constrBuilder.ConnectionString = connectionString;
			}
		}
		public string バージョン
		{
			get { return connection.ServerVersion; }
		}

		#endregion

		#region オーバロード

		[除外]
		public override IDbConnection Connection
		{
			get { return connection; }
		}

		[除外]
		public override DbDataAdapter GetDbDataAdapter(IDbCommand selectCommand)
		{
			SqliteDataAdapter adapter = new SqliteDataAdapter(selectCommand as SqliteCommand);
			return adapter;
		}
		[除外]
		public override DbCommandBuilder CreateCommandBuilder(DbDataAdapter adapter)
		{
			SqliteCommandBuilder builder = new SqliteCommandBuilder(adapter as SqliteDataAdapter);
			return builder;
		}

		[除外]
		public override IDbCommand CreateCommand(string query)
		{
			SqliteCommand command = new SqliteCommand(query, connection);
			if (transaction != null) command.Transaction = transaction;
			return command;
		}

		[除外]
		public override string[] GetTableNames()
		{
			bool keepAlive = (connection.State == ConnectionState.Open);
			if (!keepAlive) 接続(null);

			object[][] table = SelectData(".tables");
			List<string> names = new List<string>();
			foreach (object[] row in table)
			{
				names.Add(row[0].ToString());
			}

			if (!keepAlive) 切断();
			return names.ToArray();
		}

		[除外]
		public override string[] GetViewNames()
		{
			bool keepAlive = (connection.State == ConnectionState.Open);
			if (!keepAlive) 接続(null);

			object[][] table = SelectData(".tables");
			List<string> names = new List<string>();
			foreach (object[] row in table)
			{
				names.Add(row[0].ToString());
			}

			if (!keepAlive) 切断();
			return names.ToArray();
		}

		[除外]
		public override string[] GetColumnNames(string tableName)
		{
			bool keepAlive = (connection.State == ConnectionState.Open);
			if (!keepAlive) 接続(null);

			object[][] table = SelectData("PRAGMA table_info('" + tableName + "');");
			List<string> names = new List<string>();
			foreach (object[] row in table)
			{
				names.Add(row[1].ToString());
			}

			if (!keepAlive) 切断();
			return names.ToArray();
		}

		#endregion
	}
}

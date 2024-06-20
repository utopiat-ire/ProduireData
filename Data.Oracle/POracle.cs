// 日本語プログラミング言語「プロデル」データベースプラグイン
// Copyright(C) 2007-2024 utopiat.net https://github.com/utopiat-ire/
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace Produire.Data.Oracle
{
	[種類(DocUrl = "/data/db/dboracle.htm")]
	public class オラクルデータベース : データベースエンジン
	{
		#region フィールド

		string connectionString;
		string dataSource;
		string userId;
		string password;

		OracleConnection connection = new OracleConnection();
		OracleTransaction transaction;

		#endregion

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
					StringBuilder builder = new StringBuilder();
					if (!string.IsNullOrEmpty(userId))
						builder.Append("User Id=" + userId + ";");
					if (!string.IsNullOrEmpty(password))
						builder.Append("password=" + password + ";");
					if (!string.IsNullOrEmpty(dataSource))
						builder.Append("Data Source=" + dataSource + ";");

					connection.ConnectionString = builder.ToString();
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
				transaction = connection.BeginTransaction();
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
			get { return dataSource; }
			set { dataSource = value; }
		}
		public string 接続文字列
		{
			get { return connectionString; }
			set { connectionString = value; }
		}
		public string ユーザID
		{
			get { return userId; }
			set { userId = value; }
		}
		public string パスワード
		{
			get { return password; }
			set { password = value; }
		}
		public int 接続タイムアウト時間
		{
			get { return connection.ConnectionTimeout; }
		}
		public string クライアントID
		{
			set { connection.ClientId = value; }
		}

		public string バージョン
		{
			get { return connection.ServerVersion; }
		}

		#endregion

		#region オーバロードされたメソッド

		[除外]
		public override IDbConnection Connection
		{
			get { return connection; }
		}

		[除外]
		public override DbDataAdapter GetDbDataAdapter(IDbCommand selectCommand)
		{
			OracleDataAdapter adapter = new OracleDataAdapter(selectCommand as OracleCommand);
			return adapter;
		}
		[除外]
		public override DbCommandBuilder CreateCommandBuilder(DbDataAdapter adapter)
		{
			OracleCommandBuilder builder = new OracleCommandBuilder(adapter as OracleDataAdapter);
			return builder;
		}

		[除外]
		public override IDbCommand CreateCommand(string query)
		{
			OracleCommand command = new OracleCommand(query, connection);
			return command;
		}

		[除外]
		public override string[] GetTableNames()
		{
			throw new ProduireException("このエンジンでは対応していません。");
		}

		[除外]
		public override string[] GetViewNames()
		{
			throw new ProduireException("このエンジンでは対応していません。");
		}

		[除外]
		public override string[] GetColumnNames(string tableName)
		{
			throw new ProduireException("このエンジンでは対応していません。");
		}

		#endregion
	}
}

using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace ETL
{
    public class OneGramLoader : GramLoader
    {
        public void ProcessOneGram(StreamReader sr, string connectionString)
        {
            var currentMetrics = _BuildOneGramDataTable();
            var currentNGrams = NGramCommon.BuildNGramDataTable();
            var currentCount = 0L;
            while (!sr.EndOfStream)
            {
                var line = sr.ReadLine();
                var parts = line.Split('\t');

                bool isNoun;
                bool isVerb;
                bool isNumber;
                bool isAdjective;

                var ngram = _GetProperties(parts[0], out isNoun, out isVerb, out isNumber, out isAdjective);
                var hash = BuildHash(ngram);

                // skip what are numbers but not marked as numbers.
                if (ngram.All(x => Char.IsDigit(x) || x == '.' || x == ',') && !isNumber)
                    continue;


                var year = Int16.Parse(parts[1]);
                var matchCount = Int32.Parse(parts[2]);
                var bookCount = Int32.Parse(parts[3]);

                if (!currentNGrams.Rows.Contains(hash))
                    currentNGrams.Rows.Add(new object[] { hash, ngram });

                currentMetrics.Rows.Add(new object[] { hash, year, matchCount, bookCount, isNumber, isNoun, isVerb, isAdjective });
                currentCount++;

                if (currentCount % 100000 == 0)
                {
                    _BulkUploadData(connectionString, currentMetrics, currentNGrams);
                    Console.Write("\rCopies {0} entries", currentCount);
                    currentMetrics.Dispose();
                    currentMetrics = _BuildOneGramDataTable();
                    currentNGrams = NGramCommon.BuildNGramDataTable();
                }
            }

            _BulkUploadData(connectionString, currentMetrics, currentNGrams);
            Console.WriteLine("\rCopies {0} entries", currentCount);
            Console.WriteLine();
        }

        private static string _GetProperties(string rawNGram, out bool isNoun, out bool isVerb, out bool isNumber, out bool isAdjective)
        {
            isNumber = rawNGram.Contains("_NUM");
            isNoun = rawNGram.Contains("_NOUN");
            isVerb = rawNGram.Contains("_VERB");
            isAdjective = rawNGram.Contains("_ADJ");

            return rawNGram
                .Replace("_NUM", "")
                .Replace("_NOUN", "")
                .Replace("_VERB", "")
                .Replace("_ADJ", "");
        }

        private static void _BulkUploadData(string connectionString, DataTable currentMetrics, DataTable currentNGrams)
        {
            using (var copy = new SqlBulkCopy(connectionString))
            {
                copy.ColumnMappings.Add("NGramHash", "NGramHash");
                copy.ColumnMappings.Add("Year", "Year");
                copy.ColumnMappings.Add("Matches", "Matches");
                copy.ColumnMappings.Add("Books", "Books");
                copy.ColumnMappings.Add("IsNumber", "IsNumber");
                copy.ColumnMappings.Add("IsNoun", "IsNoun");
                copy.ColumnMappings.Add("IsVerb", "IsVerb");
                copy.ColumnMappings.Add("IsAdjective", "IsAdjective");
                copy.DestinationTableName = "[OneGramMetrics]";
                copy.WriteToServer(currentMetrics);
            }

            using (var copy = new SqlBulkCopy(connectionString))
            {
                copy.ColumnMappings.Add("NGramHash", "NGramHash");
                copy.ColumnMappings.Add("NGram", "NGram");
                copy.DestinationTableName = "[NGramsTmp]";
                copy.WriteToServer(currentNGrams);
            }
        }

        private static DataTable _BuildOneGramDataTable()
        {
            var dt = new DataTable();
            dt.Columns.Add("NGramHash", typeof(byte[]));
            dt.Columns.Add("Year", typeof(Int16));
            dt.Columns.Add("Matches", typeof(Int64));
            dt.Columns.Add("Books", typeof(Int64));
            dt.Columns.Add("IsNumber", typeof(bool));
            dt.Columns.Add("IsNoun", typeof(bool));
            dt.Columns.Add("IsVerb", typeof(bool));
            dt.Columns.Add("IsAdjective", typeof(bool));
            return dt;
        }
    }
}
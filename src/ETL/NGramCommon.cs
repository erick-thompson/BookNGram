using System.Data;

namespace ETL
{
    public class NGramCommon
    {
        public static DataTable BuildNGramDataTable()
        {
            var dt = new DataTable();
            dt.Columns.Add("NGramHash", typeof(byte[]));
            dt.Columns.Add("NGram", typeof(string));

            dt.PrimaryKey = new[] { dt.Columns[0] };
            return dt;
        }
    }
}
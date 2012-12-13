using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Ionic.Zip;
using Ionic.Zlib;

namespace ETL
{
    public static class Program
    {
        private static SHA1 _ShaHasher;

        public static int Main(string[] args)
        {
            var inputFile = args[0];
            var targetFolder = args[1];

            if (!Directory.Exists(targetFolder))
                Directory.CreateDirectory(targetFolder);

            var language = _GetLanguage(args);
            var type = _GetGramType(args);
            var fictionOnly = args.Any(x => x.ToLower() == "fiction");
            var versionDate = args.Single(x => x.All(Char.IsNumber));
            var includeDependencies = args.Any(x => x.ToLower() == "include0");

            var rawHrefs = File.ReadAllLines(inputFile).Select(_GetHref).Where(x => x != null).Select(x => x.ToLower());
            var filtered = rawHrefs
                .Where(x => x.Contains(_GetNGramTypeForFilter(type).ToLower()))
                .Where(x => _FilterByLanguage(x, language))
                .Where(x => x.Contains(versionDate)).ToArray();

            var connectionString = @"Data Source=.\mssql12;Initial Catalog=NGram;Integrated Security=True";

            var oneGramLoader = new OneGramLoader();

            foreach (var rawHref in filtered)
            {
                Console.WriteLine("Downloading href {0}", rawHref);
                var req = WebRequest.CreateHttp(rawHref);
                var res = req.GetResponse();
                using (var resStream = res.GetResponseStream())
                {
                    using (var zipStream = new GZipStream(resStream, CompressionMode.Decompress))
                    {
                        using (var sr = new StreamReader(zipStream))
                        {
                            oneGramLoader.ProcessOneGram(sr, connectionString);
                        }
                        
                        zipStream.Close();
                    }
                    resStream.Close();
                }
            }

            Console.WriteLine("Finished - any key");
            Console.ReadLine();

            return 0;
        }

        private static bool _FilterByLanguage(string x, Language language)
        {
            switch (language)
            {
                case Language.US:
                    return x.Contains("eng-us");
                case Language.GB:
                    return x.Contains("eng-gb");
                default:
                    return x.Contains("eng-all");
            }
        }

        private static string _GetHref(string input)
        {
            return input;
        }

        private static void _ShowUsage()
        {
            Console.WriteLine("Download and unpacks ngram");

            Console.WriteLine("ETL.EXE [input file] [taget folder] [language] [type] [fiction] [version date] [include0]");

            Console.WriteLine();
            Console.WriteLine("[language] = all|us|gb");
            Console.WriteLine("[type] = 1gram|2gram|3gram|4gram|5gram");
            Console.WriteLine("[fiction] = fiction only [optional]");
            Console.WriteLine("[version date] = yyyymmdd");
            Console.WriteLine("[include0] = include 0gram");


        }

        private static string _GetNGramTypeForFilter(NGramType nGramType)
        {
            switch (nGramType)
            {
                case NGramType.fiveGram:
                    return "5gram";
                case NGramType.fourGram:
                    return "4gram";
                case NGramType.threeGram:
                    return "3gram";
                case NGramType.twoGram:
                    return "2gram";
                case NGramType.oneGram:
                    return "1gram";
                default:
                    throw new ArgumentOutOfRangeException("nGramType");
            }
        }

        private static NGramType _GetGramType(string[] args)
        {
            var type = NGramType.Unknown;
            if (args.Any(x => x.ToLower() == "1gram"))
                type = NGramType.oneGram;
            if (args.Any(x => x.ToLower() == "2gram"))
                type = NGramType.twoGram;
            if (args.Any(x => x.ToLower() == "3gram"))
                type = NGramType.threeGram;
            if (args.Any(x => x.ToLower() == "4gram"))
                type = NGramType.fourGram;
            if (args.Any(x => x.ToLower() == "5gram"))
                type = NGramType.fiveGram;
            if (type == NGramType.Unknown)
                throw new ArgumentOutOfRangeException("args");
            return type;
        }


        private static Language _GetLanguage(string[] args)
        {
            var language = Language.Unknown;
            if (args.Any(x => x.ToLower() == "us"))
                language = Language.US;
            if (args.Any(x => x.ToLower() == "gb"))
                language = Language.GB;
            if (args.Any(x => x.ToLower() == "all" || language == Language.Unknown))
                language = Language.All;
            if (language == Language.Unknown)
            {
                Console.WriteLine("Error parsing language");
                {
                    throw new ArgumentOutOfRangeException("args");
                }
            }
            return language;
        }
    }

    public enum Language
    {
        Unknown,
        US,
        GB,
        All
    }

    public enum NGramType
    {
        Unknown,
        oneGram,
        twoGram,
        threeGram,
        fourGram,
        fiveGram,
    }
}

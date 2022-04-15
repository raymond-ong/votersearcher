using ComelecDbLib;
using ElasticSearchWriter;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleDecrypter
{
    class MySqlScriptToElastic
    {
        ComelecDbAccessor _accessor = new ComelecDbAccessor();
        ElasticManager _emgr = new ElasticManager();
        ComelecDbWriter _dbWriter = new ComelecDbWriter();
        string[] _lineSeps = new string[] {"),"};
        string[] _recordSeps = new string[] { "','","LL,", ",NU" };

        internal void Process(string dirSqlBackups)
        {
            string[] files = Directory.GetFiles(dirSqlBackups);
            int countFiles = 0;
            Parallel.ForEach(files, (file) => 
            {
                foreach (string line in File.ReadLines(file))
                {                    
                    if (!line.StartsWith("INSERT"))
                    {
                        continue;
                    }
                    ParseLine(line);
                }
                Interlocked.Increment(ref countFiles);
                Console.WriteLine($"Processed {file} - [{countFiles} / {files.Count()}]");
            });
        }

        // Parse one line
        public void ParseLine(string line)
        {
            // Each line approx 2k records
            // Insert them as 1 batch
            List<VoterInfo> infos = new List<VoterInfo>();
            //string[] records = line.Trim('\'').Split(_lineSeps, StringSplitOptions.None);
            var records = TokenizeLine(line);
            foreach(string record in records)
            {
                //string[] values = record.Trim('\'').Split(_recordSeps, StringSplitOptions.None);
                var values = TokenizeRecord(record);
                int startIdx = record.StartsWith("INSERT") ? 0 : 0;
                DateTime bday = new DateTime(1900, 1, 1); //
                try
                {
                    bday = values[startIdx + 15] == string.Empty ? new DateTime(1900, 1, 1) : new DateTime(
                            Convert.ToInt32(_accessor.DecryptWithSingleQuotes(values[startIdx + 15])),
                            Convert.ToInt32(_accessor.DecryptWithSingleQuotes(values[startIdx + 16])),
                            Convert.ToInt32(_accessor.DecryptWithSingleQuotes(values[startIdx + 17]))
                            );

                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString()); // Some bdays are invalid, e.g. Feb 29 in non-leap year
                }

                VoterInfo info = new VoterInfo()
                {
                    Lastname = _accessor.DecryptWithSingleQuotes(values[startIdx+4]),
                    Firstname = _accessor.DecryptWithSingleQuotes(values[startIdx + 5]),
                    Maternalname = _accessor.DecryptWithSingleQuotes(values[startIdx + 6]),
                    DateOfBirth = bday,
                    Address = values[startIdx + 9],
                    CivilStatus = values[startIdx + 8],
                    Sex = values[startIdx + 7]
                };
                infos.Add(info);
            }

            _dbWriter.WriteBulkToElasticDb(infos);
        }

        // Break into different records
        public List<string> TokenizeLine(string line)
        {
            bool isInsideQuote = false;
            List<string> ret = new List<string>();
            StringBuilder sbCurr = new StringBuilder();
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];

                // Process escaped characters inside quotes; preserve escapted characters first; will be processed later in TokenizeRecord
                if (isInsideQuote && c == '\\' && i < line.Length)
                {
                    sbCurr.Append(line[i++]);
                    sbCurr.Append(line[i]);
                    continue;
                }

                // BUG: 'F','S','\\',',   not able to handle address that is just backslash
                ////if (c == '\'' && line[i-1] != '\\') // exclude backslash quotes
                if (c == '\'') // For some reason, escaped characters are not captured here
                {
                    isInsideQuote = !isInsideQuote;
                }

                if (c == ')' && !isInsideQuote)
                {
                    ret.Add(sbCurr.ToString());
                    sbCurr.Clear();
                    isInsideQuote = false;
                    continue;
                }

                sbCurr.Append(c);
            }

            return ret;
        }

        // Get the fields per record
        public List<string> TokenizeRecord (string record)
        {
            int currIdx = 0;
            bool isInsideQuote = false;
            List<string> ret = new List<string>();
            StringBuilder sbCurr = new StringBuilder();
            //foreach(char c in record)
            for (int i=0; i < record.Length; i++)
            {
                char c = record[i];

                if (c == '\\' && record.Length >= i+1) // backslash escape
                {
                    sbCurr.Append(record[i + 1]);
                    i++;
                    continue;
                }

                if (c == '\'')
                {
                    isInsideQuote = !isInsideQuote;
                    continue;
                }
                
                if (c == ',' && !isInsideQuote && i > 0) // i>0: preevent first character
                {
                    ret.Add(sbCurr.ToString());
                    sbCurr.Clear();                    
                    continue;
                }

                sbCurr.Append(c);
            }

            return ret;
        }
    }
}

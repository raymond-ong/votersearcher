using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearchWriter
{
    class Program
    {
        static void Main(string[] args)
        {
            ElasticManager emgr = new ElasticManager();
            emgr.CreateIndex();

            VoterInfo newVoter = new VoterInfo
            {
                Lastname = "Lee",
                Firstname = "Kuan Yew",
                Maternalname = "Z",
                Address = "Singapore",
                CivilStatus = "M",
                DateOfBirth = DateTime.Parse("1935-01-01"),
                Sex = "M",
            };

            emgr.CreateNewVoter(newVoter);

            //VoterInfo query = new VoterInfo
            //{

            //};

            //emgr.Query(null);
            //emgr.Query2();
            //emgr.Query3("raymond", "", "ong", "");
            //ComelecDbWriter dbWriter = new ComelecDbWriter();
            //dbWriter.PopulateElasticDb();
            //dbWriter.PopulateElasticDbBulk();
            Console.ReadLine();
        }
    }
}

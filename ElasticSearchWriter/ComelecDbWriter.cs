using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ComelecDbLib;

namespace ElasticSearchWriter
{
    public class ComelecDbWriter
    {
        ComelecDbAccessor mysqlDbAccess = null;
        ElasticManager emgr = new ElasticManager();

        public ComelecDbWriter()
        {
            mysqlDbAccess = new ComelecDbAccessor();
        }

        public void PopulateElasticDb()
        {
            if (!mysqlDbAccess.Connect())
            {
                Console.WriteLine("Unable to connect to mysql db");
            }
            IEnumerable<VoterInfoComelec> listDbVoters = mysqlDbAccess.IterateAll2();
            int i = 0;
            long currTime = Environment.TickCount;
            foreach(VoterInfoComelec voter in listDbVoters)
            {
                VoterInfo emVoter = MapVoterInfo(voter);
                //emgr.CreateNewVoter(emVoter);
                if (i % 10000 == 0)
                {
                    Console.WriteLine("Written {0} out of 75302673 records to elastic, took {1}", i, Environment.TickCount - currTime);
                    currTime = Environment.TickCount;
                }
                i++;
            }

            mysqlDbAccess.Disconnect();
        }

        public void WriteBulkToElasticDb(List<VoterInfo> listDbVoters)
        {
            emgr.CreateNewVoterBulk(listDbVoters);
        }

        internal void PopulateElasticDbBulk()
        {
            if (!mysqlDbAccess.Connect())
            {
                Console.WriteLine("Unable to connect to mysql db");
            }
            IEnumerable<VoterInfoComelec> listDbVoters = mysqlDbAccess.IterateAll2();
            int i = 48720000 + 1;
            long currTime = Environment.TickCount;
            List<VoterInfo> bulkVoters = new List<VoterInfo>();
            foreach (VoterInfoComelec voter in listDbVoters)
            {
                //if (i < 39240000)
                //{
                //    i++;
                //    continue;
                //}

                VoterInfo emVoter = MapVoterInfo(voter);
                bulkVoters.Add(emVoter);
                //emgr.CreateNewVoter(emVoter);
                if (i % 10000 == 0)
                {
                    emgr.CreateNewVoterBulk(bulkVoters);
                    Console.WriteLine("Written {0} out of 75302673 records to elastic, took {1}", i, Environment.TickCount - currTime);
                    currTime = Environment.TickCount;
                    bulkVoters.Clear();
                }
                i++;
            }

            mysqlDbAccess.Disconnect();
        }

        public VoterInfo MapVoterInfo(VoterInfoComelec mySqlVoter)
        {
            return new VoterInfo
            {
                Address = mySqlVoter.Address,
                CivilStatus = mySqlVoter.CivilStatus,
                DateOfBirth = mySqlVoter.DateOfBirth,
                Firstname = mySqlVoter.Firstname,
                Lastname = mySqlVoter.Lastname,
                Maternalname = mySqlVoter.Maternalname,
                Sex = mySqlVoter.Sex
            };
        }
    }
}

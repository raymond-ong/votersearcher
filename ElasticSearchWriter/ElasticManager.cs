using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nest;

namespace ElasticSearchWriter
{
    public class ElasticManager
    {
        public Uri node;
        public ConnectionSettings settings;
        public ElasticClient client;

        //public readonly string INDEXNAME_VOTERSEARCHER = "votersearcher";
        public readonly string INDEXNAME_VOTERSEARCHER = "votersearcher2";

        public ElasticManager()
        {
            node = new Uri("http://localhost:9200");
            settings = new ConnectionSettings(node);
            settings.DisableDirectStreaming(); // for debugging
            settings.DefaultIndex(INDEXNAME_VOTERSEARCHER);
            client = new ElasticClient(settings);
        }

        public void Connect()
        {
        }

        public void CreateIndex()
        {
            IndexSettings indexSettings = new IndexSettings();
            indexSettings.NumberOfReplicas = 1;
            indexSettings.NumberOfShards = 1;

            IndexState indexState = new IndexState
            {
                Settings = indexSettings
            };

            if (client.IndexExists(INDEXNAME_VOTERSEARCHER).Exists)
            {
                Console.WriteLine("Index already exists...deleting existing index...");
                client.DeleteIndex(INDEXNAME_VOTERSEARCHER);
                //return;
            }

            ICreateIndexResponse response = client.CreateIndex(INDEXNAME_VOTERSEARCHER, c => c
            .InitializeUsing(indexState)
            .Mappings(m => m.Map<VoterInfo>(mp => mp.AutoMap())));
        }

        public void CreateNewVoter(VoterInfo voter)
        {
            client.Index(voter);
        }
        public void CreateNewVoterBulk(IEnumerable<VoterInfo> voters)
        {
            //client.Index(voter);

            //ElasticsearchProject 

            //BulkRequest bulkRequest = new BulkRequest();
            //bulkRequest.Operations = new List<IBulkOperation>();
            //BulkIndexOperation<>
            ////bulkRequest.Operations.Add()

            //client.Bulk(bulkRequest);

            client.IndexMany<VoterInfo>(voters);
        }
        

        public void Query(VoterInfo searchObj)
        {
            var result = client.Search<VoterInfo>(s => s.Query(q => q.Terms(
                c => c.Name("named_query")
                .Boost(1.1)
                .Field(p => p.Firstname)
                .Terms("kobe"))));
            //string[] values = { "kobe", "brook"}; // cannot capitalize first letter! e.g. Kobe even if the db entry is Kpbe
            //var result = client.Search<VoterInfo>(s => s.Query(q => q.Terms(c => c.Field("firstname").Terms(values))));
            //var result = client.Search<VoterInfo>(s => s.Query(q => q.ConstantScore(c => c.Filter(f => f.Terms(t => t.Field(fld => fld.Firstname).Terms(values))))));

            DisplayResult(result);
        }

        public void QueryAll()
        {
            var response = client.Search<VoterInfo>(s => s
                .Size(5)
                .Query(q => q.MatchAll()));

            DisplayResult(response);
        }

        public void Query2()
        {
            //string jsonQuery = "{\"bool\":{\"should\":[{\"match\":{\"firstname\":\"raymond aldwin\"}},{\"match\":{\"lastname\":\"ong\"}}]}}";
            string jsonQuery = "{\"bool\":{\"should\":[{\"match\":{\"firstname\":\"joel\"}},{\"match\":{\"lastname\":\"carvajal\"}}]}}";
            //string jsonQuery = "{\"bool\":{\"should\":[{\"match\":{\"maternalname\":\"policarpio\"}},{\"match\":{\"lastname\":\"martinez\"}}]}}";
            //string jsonQuery = "{\"bool\":{\"should\":[{\"match\":{\"address\":\"198A BANLAT ROAD\"}}]}}";


            var response = client.Search<VoterInfo>(s => s
                .Size(100)
                .Query(q => q.Raw(jsonQuery)));


            DisplayResult(response);
        }

        public List<VoterInfo> Query3(string firstname, string maternalname, string lastname, string address)
        {
            string jsonQuery = "{{\"bool\":{{\"should\":[{0}]}}}}";
            string strFirstNameQry = string.IsNullOrEmpty(firstname) ? null : string.Format("{{\"match\":{{\"firstname\":\"{0}\"}}}}", firstname);
            string maternalnameQry = string.IsNullOrEmpty(maternalname) ? null : string.Format("{{\"match\":{{\"maternalname\":\"{0}\"}}}}", maternalname);
            string lastnameQry = string.IsNullOrEmpty(lastname) ? null : string.Format("{{\"match\":{{\"lastname\":\"{0}\"}}}}", lastname);
            string addressQry = string.IsNullOrEmpty(address) ? null : string.Format("{{\"match\":{{\"address\":\"{0}\"}}}}", address);

            List<string> queries = new List<string>() { strFirstNameQry , maternalnameQry, lastnameQry, addressQry };
            queries.RemoveAll(x => x == null);
            string csvParams = string.Join(",", queries);
            jsonQuery = string.Format(jsonQuery, csvParams);
            var response = client.Search<VoterInfo>(s => s
                .Size(100)
                .Query(q => q.Raw(jsonQuery)));

            //DisplayResult(response);
            return MapResults(response);
        }

        private List<VoterInfo> MapResults(ISearchResponse<VoterInfo> response)
        {
            List<VoterInfo> retList = new List<VoterInfo>();

            int i = 0;
            Console.WriteLine("Hits: {0}", response.Total);
            if (response.OriginalException != null)
            {
                Console.WriteLine(response.OriginalException);
                var stream = new System.IO.MemoryStream();
                var jsonQuery = System.Text.Encoding.UTF8.GetString(response.ApiCall.RequestBodyInBytes);
                Console.WriteLine(jsonQuery);
                return retList;
            }
            foreach (var hit in response.Hits)
            {
                VoterInfo newVoter = new VoterInfo()
                {
                    Address = hit.Source.Address,
                    Firstname = hit.Source.Firstname,
                    Maternalname = hit.Source.Maternalname,
                    Lastname = hit.Source.Lastname,
                    DateOfBirth = hit.Source.DateOfBirth,
                    Sex = hit.Source.Sex,
                    CivilStatus = hit.Source.CivilStatus,                    
                };

                retList.Add(newVoter);
            }

            return retList;
        }

        public void Query1()
        {
            var response = client.Search<VoterInfo>(s => s
                .Size(5)
                .Query(q => q.Match(m => m
                    .Field(f => f.Firstname)
                    .Query("Yew"))
                    )
                    );

            DisplayResult(response);
        }

        public void DisplayResult(ISearchResponse<VoterInfo> response)
        {
            int i = 0;
            Console.WriteLine("Hits: {0}", response.Total);
            if (response.OriginalException != null)
            {
                Console.WriteLine(response.OriginalException);
                var stream = new System.IO.MemoryStream();
                var jsonQuery = System.Text.Encoding.UTF8.GetString(response.ApiCall.RequestBodyInBytes);
                Console.WriteLine(jsonQuery);
            }
            foreach (var hit in response.Hits)
            {
                Console.WriteLine("[{0}] L: {1}, M: {2}, F: {3}, Dob: {4}, A:{5}",
                    i++,
                    hit.Source.Lastname,
                    hit.Source.Maternalname,
                    hit.Source.Firstname,
                    hit.Source.DateOfBirth,
                    hit.Source.Address);
            }
        }
        public void DeleteIndex()
        {

        }

    }
}

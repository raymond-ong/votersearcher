using ComelecDbLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using models = VoterSearcher.Models;
using PagedList;
using System.Diagnostics;
using elastic = ElasticSearchWriter;

namespace VoterSearcher.Controllers
{
    public class HomeController : Controller
    {
        //[Authorize]
        public ActionResult Index()
        {
            Debug.WriteLine("HashCode: {0}", this.GetHashCode());

            if (Request.IsAjaxRequest())
            {
                // default values
                int pageNumber = 1;
                string sortBy = "FirstName";
                string SortOrder = null;
                string ExactMatch = Request.Params["exactmatch"] != null ? Request.Params["exactmatch"] : "Exact";
                try
                {
                    if (Request.Params["page"] != null)
                    {
                        pageNumber = Convert.ToInt32(Request.Params["page"]);
                    }
                    if (Request.Params["sortBy"] != null)
                    {
                        sortBy = Request.Params["sortBy"];
                    }
                    if (Request.Params["SortOrder"] != null)
                    {
                        SortOrder = Request.Params["SortOrder"];
                    }
                }
                catch (Exception ex)
                {
                    // just swallow exception
                }

#if true
                IEnumerable<models.VoterInfo> sortedVoters = null;
                if (ExactMatch == "Exact")
                {
                    ComelecDbAccessor dbAccess = new ComelecDbAccessor();
                    dbAccess.Connect();
                    //List<VoterInfoComelec> listDbVoters = dbAccess.SearchData(
                    List<VoterInfoComelec> listDbVoters = dbAccess.SearchDataIterator(
                        Request.Params["FirstName"],
                        Request.Params["MaternalName"],
                        Request.Params["LastName"]).ToList();

                    sortedVoters = SortVoters(listDbVoters, sortBy, SortOrder);
                }
                else
                {
                    elastic.ElasticManager emgr = new elastic.ElasticManager();
                    List<ElasticSearchWriter.VoterInfo> elasticResultList = emgr.Query3(
                        Request.Params["FirstName"],
                        Request.Params["MaternalName"],
                        Request.Params["LastName"],
                        Request.Params["Address"]);

                    sortedVoters = SortVotersElastic(elasticResultList, Request.Params["sortBy"], SortOrder);
                }
                var pagedList = sortedVoters.ToPagedList(pageNumber, 10);
                //return PartialView("SearchResult", pagedList);
                return PartialView("SearchResult", sortedVoters);
#else
                List<VoterInfo> listDbVoters = GeneratelistDbVoters();
                IEnumerable<VoterInfo> sortedVoters = Sort(listDbVoters, sortBy, SortOrder);
                var pagedList = sortedVoters.ToPagedList(pageNumber, 10);
                return PartialView("SearchResult", pagedList);
#endif                
            }
            else
            {
                return View();
            }
        }

        // Sort and transform list
        private IEnumerable<models.VoterInfo> SortVoters(List<VoterInfoComelec> listDbVoters, string sortBy, string SortOrder)
        {
            IEnumerable<VoterInfoComelec> sortedDbList = null;

            if (sortBy == "LastName" || string.IsNullOrEmpty(sortBy))
            {
                if (SortOrder == "DESC")
                    sortedDbList = listDbVoters.OrderByDescending(x => x.Lastname);
                else
                    sortedDbList = listDbVoters.OrderBy(x => x.Lastname);
            }
            else if (sortBy == "MaternalName")
            {
                if (SortOrder == "DESC")
                    sortedDbList = listDbVoters.OrderByDescending(x => x.Maternalname);
                else
                    sortedDbList = listDbVoters.OrderBy(x => x.Maternalname);
            }
            else if (sortBy == " BirthDate")
            {
                if (SortOrder == "DESC")
                    sortedDbList = listDbVoters.OrderByDescending(x => x.DateOfBirth);
                else
                    sortedDbList = listDbVoters.OrderBy(x => x.DateOfBirth);
            }
            else //(sortBy == "FirstName" || string.IsNullOrEmpty(sortBy))
            {
                if (SortOrder == "DESC")
                    sortedDbList = listDbVoters.OrderByDescending(x => x.Firstname);
                else
                    sortedDbList = listDbVoters.OrderBy(x => x.Firstname);
            }

            return Transform(sortedDbList);
        }
        private IEnumerable<models.VoterInfo> SortVotersElastic(List<elastic.VoterInfo> listDbVoters, string sortBy, string SortOrder)
        {
            IEnumerable<elastic.VoterInfo> sortedDbList = null;

            if (sortBy == "LastName")
            {
                if (SortOrder == "DESC")
                    sortedDbList = listDbVoters.OrderByDescending(x => x.Lastname);
                else
                    sortedDbList = listDbVoters.OrderBy(x => x.Lastname);
            }
            else if (sortBy == "MaternalName")
            {
                if (SortOrder == "DESC")
                    sortedDbList = listDbVoters.OrderByDescending(x => x.Maternalname);
                else
                    sortedDbList = listDbVoters.OrderBy(x => x.Maternalname);
            }
            else if (sortBy == " BirthDate")
            {
                if (SortOrder == "DESC")
                    sortedDbList = listDbVoters.OrderByDescending(x => x.DateOfBirth);
                else
                    sortedDbList = listDbVoters.OrderBy(x => x.DateOfBirth);
            }
            else if (sortBy == "FirstName")
            {
                if (SortOrder == "DESC")
                    sortedDbList = listDbVoters.OrderByDescending(x => x.Firstname);
                else
                    sortedDbList = listDbVoters.OrderBy(x => x.Firstname);
            }
            else
            {
                // sorted by relevance
                sortedDbList = listDbVoters;
            }

            return TransformElastic(sortedDbList);
        }
        private IEnumerable<models.VoterInfo> Transform(IEnumerable<VoterInfoComelec> sortedDbList)
        {
            foreach(VoterInfoComelec dbItem in sortedDbList)
            {
                yield return new models.VoterInfo()
                {
                    FirstName = dbItem.Firstname,
                    LastName = dbItem.Lastname,
                    MaternalName = dbItem.Maternalname,
                    CivilStatus = dbItem.CivilStatus,
                    Address = dbItem.Address,
                    DateOfBirth = dbItem.DateOfBirth
                };
            }
        }

        private IEnumerable<models.VoterInfo> TransformElastic(IEnumerable<elastic.VoterInfo> sortedDbList)
        {
            foreach (elastic.VoterInfo dbItem in sortedDbList)
            {
                yield return new models.VoterInfo()
                {
                    FirstName = dbItem.Firstname,
                    LastName = dbItem.Lastname,
                    MaternalName = dbItem.Maternalname,
                    CivilStatus = dbItem.CivilStatus,
                    Address = dbItem.Address,
                    DateOfBirth = dbItem.DateOfBirth
                };
            }
        }

        [Authorize]
        public ActionResult About()
        {
            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Your contact page.";

            return View();
        }
    }
}
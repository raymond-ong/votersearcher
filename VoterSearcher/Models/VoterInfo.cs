using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Web;

namespace VoterSearcher.Models
{
    public class VoterInfo
    {
        [DisplayName("First Name")]
        public string FirstName { get; set; }

        [DisplayName("Maternal Name")]
        public string MaternalName { get; set; }

        [DisplayName("Last Name")]
        public string LastName { get; set; }

        [DisplayName("Birth Year")]
        public ushort DobYear { get; set; }

        [DisplayName("Birth Month")]
        public ushort DobMonth { get; set; }

        [DisplayName("Birth Day")]
        public ushort DobDay { get; set; }

        [DisplayName("Address")]
        public string Address { get; set; }

        [DisplayName("Civil Status")]
        public string CivilStatus { get; set; }

        [DisplayName("Date of Birth")]
        public DateTime DateOfBirth { get; set; }
    }
}
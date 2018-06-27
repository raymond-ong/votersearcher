using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(VoterSearcher.Startup))]
namespace VoterSearcher
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}

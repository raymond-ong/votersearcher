using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RenameHjSplitFiles
{
    /// <summary>
    /// We HJ splitted the comweb database into 3466 files.
    /// We want to import explore 7therbv2 table only.
    /// We have a list of HJ Split files.
    /// Starting from .3108 to 3466.
    /// Task is to rename 3108 to 001
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            string[] files = System.IO.Directory.GetFiles(@"H:\comweb7therbv2");
            foreach(string file in files)
            {
                string strFileNum = file.Substring(file.LastIndexOf('.') + 1, file.Length - file.LastIndexOf('.') - 1);
                int fileNum = Convert.ToInt32(strFileNum);
                string newName = string.Format("{0}.{1:000}", file.Substring(0, file.LastIndexOf('.')), fileNum - 3107);
                Console.WriteLine("{0} -> {1}", file, newName);
                System.IO.File.Move(file, newName);
            }

            Console.ReadLine();
        }
    }
}

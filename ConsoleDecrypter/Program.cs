using ComelecDbLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleDecrypter
{
    class Program
    {
        static void Main(string[] args)
        {
            //ComelecDbAccessor accessor = new ComelecDbAccessor();
            MySqlScriptToElastic mysqlToElastic = new MySqlScriptToElastic();
            mysqlToElastic.Process("E:\\comweb7therbv2");
            //mysqlToElastic.Process("E:\\Comelec_dbg");
            //TestDecrypt();
            Console.ReadLine();
        }

        static void TestDecrypt()
        {
            ComelecDbAccessor accessor = new ComelecDbAccessor();

            Console.WriteLine(accessor.Decrypt("x1k+kxOif1rh0tQ15BB7bg==")); //ABAYA
            Console.WriteLine(accessor.Decrypt("jpzVTzStARnj5VMd54v/hw==")); //MAYROSE
            Console.WriteLine(accessor.Decrypt("/pX+t3i8q729hge6eXk6aw==")); //PALIGAT
            Console.WriteLine(accessor.Decrypt("smORB8XkrrVnr0jOrklnOQ==")); //1960
            Console.WriteLine(accessor.Decrypt("+5Fpo336fX+Bg6LNALdGQg==")); //08
            Console.WriteLine(accessor.Decrypt("w0jZKws6YFay1TzkmQCR9A==")); //24

            Console.WriteLine(accessor.Decrypt("p6caH4Y5oMTEVSGs2OxylQ==")); //8101
            Console.WriteLine(accessor.Decrypt("BvJagwUsT4Fu+z5s5J2/MQ==")); //0005A
            Console.WriteLine(accessor.Decrypt("seKB9ibxWZPgTLE047izkw==")); //H2460MPA20000

            Console.WriteLine(accessor.Decrypt("v9bS28Ycyq6c383 / GQTaNw =="));             

            Console.ReadLine();

        }
    }
}

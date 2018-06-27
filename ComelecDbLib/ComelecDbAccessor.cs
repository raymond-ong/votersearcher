using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ComelecDbLib
{
    public class ComelecDbAccessor
    {
        string _connStr = "server=127.0.0.1;uid=root; pwd=root;database=comweb; ConnectionTimeout=10000";
        MySqlConnection _dbClientRead = null;
        MySqlConnection _dbClientUpdate = null;
        string _querySearch = "SELECT LASTNAME, FIRSTNAME, MATERNALNAME,"+
            "SEX, CIVILSTATUS, RESSTREET, " +
            "DOBYEAR, DOBMONTH, DOBDAY, " +
            "REG_DATE, UPDATE_TIME, VINP1, VINP2, VINP3 FROM comweb.webvs_7therbv2 " +
            "where FIRSTNAME = {0} and " +
            "MATERNALNAME = {1} " +
            "and LASTNAME= {2} LIMIT 0, 1000; ";
        string _querySearch2 = "SELECT LASTNAME, FIRSTNAME, MATERNALNAME," +
            "SEX, CIVILSTATUS, RESSTREET, " +
            "DOBYEAR, DOBMONTH, DOBDAY, " +
            "REG_DATE, UPDATE_TIME, VINP1, VINP2, VINP3 FROM comweb.webvs_7therbv2 " +
            "where FIRSTNAME_P like '{0}' and " +
            "MATERNALNAME_P {1} " +
            "and LASTNAME_P = '{2}'; ";
        //string _queryAll = "SELECT LASTNAME, FIRSTNAME, MATERNALNAME," +
        //    "SEX, CIVILSTATUS, RESSTREET, " +
        //    "DOBYEAR, DOBMONTH, DOBDAY, " +
        //    "REG_DATE, UPDATE_TIME, VINP1, VINP2, VINP3 FROM comweb.webvs_7therbv2 ";
        string _queryAll = "SELECT LASTNAME, FIRSTNAME, MATERNALNAME," +
            "SEX, CIVILSTATUS, RESSTREET, " +
            "DOBYEAR, DOBMONTH, DOBDAY, " +
            "REG_DATE, UPDATE_TIME, VINP1, VINP2, VINP3 FROM comweb.webvs_7therbv2 LIMIT 50000000 OFFSET 48720000";
        string _strKey = "~s1N1g@nGN@m@N0k!~MayH@l0ngB@b0y";
        string _strIv = "S1n1G@NgN@B@b0y!";

        byte[] _byteKey = null;
        byte[] _byteIv = null;

        public ComelecDbAccessor()
        {
            _byteKey = Encoding.ASCII.GetBytes(_strKey);
            _byteIv = Encoding.ASCII.GetBytes(_strIv);
        }

        public bool Connect()
        {
            _dbClientRead = new MySqlConnection();
            _dbClientRead.ConnectionString = _connStr;
            try
            {
                _dbClientRead.Open();
            }
            catch(Exception ex)
            {
                Console.WriteLine("Cannot connect to MySQL DB: " + ex.Message);
                return false;
            }

            return true;
        }

        private bool ConnectUpdater()
        {
            _dbClientUpdate = new MySqlConnection();
            _dbClientUpdate.ConnectionString = _connStr;
            try
            {
                _dbClientUpdate.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot connect to MySQL DB: " + ex.Message);
                return false;
            }

            return true;
        }

        public void Disconnect()
        {
            try
            {
                _dbClientRead.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot disconnect MySQL DB: " + ex.Message);
            }
        }

        public void DisconnectUpdater()
        {
            try
            {
                _dbClientUpdate.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot disconnect MySQL DB: " + ex.Message);
            }
        }

        public string Decrypt(string cipherText)
        {
            byte[] cipherTextBytes = Convert.FromBase64String(cipherText);
            return DecryptStringFromBytes(cipherTextBytes, _byteKey, _byteIv);
        }

        public string Encrypt(string plainText)
        {
            byte[] encBytes = EncryptStringToBytes(plainText, _byteKey, _byteIv);

            return Convert.ToBase64String(encBytes);
        }

        static byte[] EncryptStringToBytes(string plainText, byte[] Key, byte[] IV)
        {
            // Check arguments.
            if (plainText == null || plainText.Length <= 0)
                throw new ArgumentNullException("plainText");
            if (Key == null || Key.Length <= 0)
                throw new ArgumentNullException("Key");
            if (IV == null || IV.Length <= 0)
                throw new ArgumentNullException("Key");
            byte[] encrypted;
            // Create an Rijndael object
            // with the specified key and IV.
            using (Rijndael rijAlg = Rijndael.Create())
            {
                rijAlg.Key = Key;
                rijAlg.IV = IV;
                rijAlg.Padding = PaddingMode.Zeros;

                // Create a decrytor to perform the stream transform.
                ICryptoTransform encryptor = rijAlg.CreateEncryptor(rijAlg.Key, rijAlg.IV);

                // Create the streams used for encryption.
                using (MemoryStream msEncrypt = new MemoryStream())
                {
                    using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                    {
                        using (StreamWriter swEncrypt = new StreamWriter(csEncrypt))
                        {

                            //Write all data to the stream.
                            swEncrypt.Write(plainText);
                        }
                        encrypted = msEncrypt.ToArray();
                    }
                }
            }


            // Return the encrypted bytes from the memory stream.
            return encrypted;

        }
        static string DecryptStringFromBytes(byte[] cipherText, byte[] Key, byte[] IV)
        {
            // Check arguments.
            if (cipherText == null || cipherText.Length <= 0)
                //throw new ArgumentNullException("cipherText");
                return null;
            if (Key == null || Key.Length <= 0)
                //throw new ArgumentNullException("Key");
                return null;
            if (IV == null || IV.Length <= 0)
                //throw new ArgumentNullException("Key");
                return null;

            // Declare the string used to hold
            // the decrypted text.
            string plaintext = null;

            // Create an Rijndael object
            // with the specified key and IV.
            using (Rijndael rijAlg = Rijndael.Create())
            {
                rijAlg.Key = Key;
                rijAlg.IV = IV;
                rijAlg.Padding = PaddingMode.None;
                //rijAlg.KeySize = 128;
                //rijAlg.BlockSize = 256;

                // Create a decrytor to perform the stream transform.
                ICryptoTransform decryptor = rijAlg.CreateDecryptor(rijAlg.Key, rijAlg.IV);

                // Create the streams used for decryption.
                using (MemoryStream msDecrypt = new MemoryStream(cipherText))
                {
                    using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                    {
                        using (StreamReader srDecrypt = new StreamReader(csDecrypt))
                        {

                            // Read the decrypted bytes from the decrypting stream
                            // and place them in a string.
                            plaintext = srDecrypt.ReadToEnd();
                        }
                    }
                }

            }

            return plaintext.Trim(new char[] {'\0'});
            //return 
        }

        public IEnumerable<VoterInfoComelec> SearchDataIterator(string inFirstName, string inMaternalName, string inLastName)
        {
            string qryFirstName = string.IsNullOrEmpty(inFirstName) ? "FIRSTNAME" : string.Format("'{0}'", Encrypt(inFirstName.ToUpper()));
            string qryMaternalName = string.IsNullOrEmpty(inMaternalName) ? "MATERNALNAME" : string.Format("'{0}'", Encrypt(inMaternalName.ToUpper()));
            string qryLastName = string.IsNullOrEmpty(inLastName) ? "LASTNAME" : string.Format("'{0}'", Encrypt(inLastName.ToUpper()));
            string querySearch = string.Format(_querySearch, qryFirstName, qryMaternalName, qryLastName);

            MySqlCommand cmd = new MySqlCommand(querySearch, _dbClientRead);
            MySqlDataReader dataReader = cmd.ExecuteReader();
            while (dataReader.Read())
            {
                VoterInfoComelec voterInfo = new VoterInfoComelec();
                voterInfo.Lastname = Decrypt(dataReader[0].ToString());
                voterInfo.Firstname = Decrypt(dataReader[1].ToString());
                voterInfo.Maternalname = Decrypt(dataReader[2].ToString());

                voterInfo.Sex = dataReader[3].ToString();
                voterInfo.CivilStatus = dataReader[4].ToString();
                voterInfo.Address = dataReader[5].ToString();

                string bdayYear = Decrypt(dataReader[6].ToString());
                string bdayMonth = Decrypt(dataReader[7].ToString());
                string bdayDay = Decrypt(dataReader[8].ToString());

                voterInfo.DateOfBirth = new DateTime(
                        Convert.ToInt32(bdayYear),
                        Convert.ToInt32(bdayMonth),
                        Convert.ToInt32(bdayDay)
                    );


                yield return voterInfo;
            }

            dataReader.Close();
            Console.WriteLine("Closed Connection");
        }

        public List<VoterInfoComelec> SearchData(string inFirstName, string inMaternalName, string inLastName)
        {
            List<VoterInfoComelec> retList = new List<VoterInfoComelec>();
            string qryFirstName = string.IsNullOrEmpty(inFirstName) ? "FIRSTNAME" : string.Format("'{0}'", Encrypt(inFirstName.ToUpper()));
            string qryMaternalName = string.IsNullOrEmpty(inMaternalName) ? "MATERNALNAME" : string.Format("'{0}'", Encrypt(inMaternalName.ToUpper()));
            string qryLastName = string.IsNullOrEmpty(inLastName) ? "LASTNAME" : string.Format("'{0}'", Encrypt(inLastName.ToUpper()));
            string querySearch = string.Format(_querySearch, qryFirstName, qryMaternalName, qryLastName);

            MySqlCommand cmd = new MySqlCommand(querySearch, _dbClientRead);
            MySqlDataReader dataReader = cmd.ExecuteReader();
            while (dataReader.Read())
            {
                VoterInfoComelec voterInfo = new VoterInfoComelec();
                voterInfo.Lastname = Decrypt(dataReader[0].ToString());
                voterInfo.Firstname = Decrypt(dataReader[1].ToString());
                voterInfo.Maternalname = Decrypt(dataReader[2].ToString());

                voterInfo.Sex = dataReader[3].ToString();
                voterInfo.CivilStatus = dataReader[4].ToString();
                voterInfo.Address = dataReader[5].ToString();

                string bdayYear = Decrypt(dataReader[6].ToString());
                string bdayMonth = Decrypt(dataReader[7].ToString());
                string bdayDay = Decrypt(dataReader[8].ToString());

                voterInfo.DateOfBirth = new DateTime(
                        Convert.ToInt32(bdayYear),
                        Convert.ToInt32(bdayMonth),
                        Convert.ToInt32(bdayDay)
                    );


                retList.Add(voterInfo);
            }

            return retList;
        }

        public void SetDecryptedInfo()
        {            
            ConnectUpdater();
            string querySelectCount = "SELECT COUNT(*) FROM comweb.webvs_7therbv2";
            string querySelectAll = "SELECT LASTNAME, FIRSTNAME, MATERNALNAME, N_ID from comweb.webvs_7therbv2 LIMIT {0}, {1}";

            MySqlCommand cmdSelectCount = new MySqlCommand(querySelectCount, _dbClientRead);
            MySqlDataReader dataReader = cmdSelectCount.ExecuteReader();
            dataReader.Read();
            int totalCount = Convert.ToInt32(dataReader[0].ToString());
            dataReader.Close();

            // read per 1000 records
            for (int i = 10100000; i < totalCount; i += 1000000)
            {
                long ticksStart = Environment.TickCount;
                MySqlCommand cmdSelect = new MySqlCommand(string.Format(querySelectAll, i, 1000000), _dbClientRead);
                cmdSelect.CommandTimeout = 0;
                dataReader = cmdSelect.ExecuteReader();
                int ctr = i;
                List<VoterInfoDecrypted> batchEntry = new List<VoterInfoDecrypted>(1000000);
                while (dataReader.Read())
                {
                    string txtLastName = Decrypt(dataReader[0].ToString());
                    string txtFirstName = Decrypt(dataReader[1].ToString());
                    string txtMaternalName = Decrypt(dataReader[2].ToString());
                    string nId = dataReader[3].ToString();               

                    batchEntry.Add(new VoterInfoDecrypted 
                                    {
                                        FirstName = txtFirstName,
                                        LastName = txtLastName,
                                        MaternalName = txtMaternalName,
                                        Nid = nId
                                    });
                }

                long ticksRead = Environment.TickCount;

                dataReader.Close();

                foreach(VoterInfoDecrypted info in batchEntry)
                {
                    UpdateRecord(info.FirstName, info.LastName, info.MaternalName, info.Nid);
                }

                Console.WriteLine("Updated {0} Read: {1}, Write: {2}", i, ticksRead - ticksStart, Environment.TickCount - ticksRead);
            }

            DisconnectUpdater();


#if false
            MySqlCommand cmdSelect = new MySqlCommand(querySelectAll, _dbClientRead);
            
            MySqlDataReader dataReader = cmdSelect.ExecuteReader();
            int ctr = 0;
            while (dataReader.Read())
            {
                string txtLastName = Decrypt(dataReader[0].ToString());
                string txtFirstName = Decrypt(dataReader[1].ToString());
                string txtMaternalName = Decrypt(dataReader[2].ToString());
                string nId = dataReader[3].ToString();               

#if true
                SetDecryptedInfoTest(txtFirstName, txtLastName, txtMaternalName, nId);
#else
                string updateQueryCurr = string.Format(queryUpdate, txtLastName, txtFirstName, txtMaternalName, nId);
                MySqlCommand cmdUpdate = new MySqlCommand(updateQueryCurr, _dbClientUpdate);
                cmdUpdate.ExecuteNonQuery();
#endif

                Console.WriteLine("Updated: " + ctr++);
            }

            DisconnectUpdater();
#endif
        }

        public void UpdateRecord(string firstName, string lastName, string maternalName, string nId)
        {
            //ConnectUpdater();
            if (_dbClientUpdate == null || _dbClientUpdate.State == System.Data.ConnectionState.Closed)
            {
                ConnectUpdater();
            }
            string queryUpdate = "UPDATE comweb.webvs_7therbv2 SET LASTNAME_P = @LastName, FIRSTNAME_P = @FirstName, MATERNALNAME_P = @MaternalName WHERE N_ID = @Nid LIMIT 1";

            //string updateQueryCurr = string.Format(queryUpdate, lastName, firstName, maternalName, nId);
            MySqlCommand cmdUpdate = new MySqlCommand(queryUpdate, _dbClientUpdate);
            cmdUpdate.CommandTimeout = 0;
            cmdUpdate.Parameters.Add("@LastName", lastName);
            cmdUpdate.Parameters.Add("@FirstName", firstName);
            cmdUpdate.Parameters.Add("@MaternalName", maternalName);
            cmdUpdate.Parameters.Add("@Nid", nId);

            cmdUpdate.ExecuteNonQuery();            
        }

        private void PrintDetails(Dictionary<string, string> voterDetails)
        {
            Console.WriteLine();
            foreach(var kvp in voterDetails)
            {
                if (kvp.Key.IndexOf("NAME") == -1)
                {
                    //continue;
                }
                Console.WriteLine("{0} \t\t\t\t\t{1}", kvp.Key, kvp.Value);
            }            
        }

        public IEnumerable<Dictionary<string, string>> IterateAll()
        {
            //List<Dictionary<string, string>> retList = new List<Dictionary<string, string>>();
            string querySearch = string.Format(_queryAll);

            MySqlCommand cmd = new MySqlCommand(querySearch, _dbClientRead);
            MySqlDataReader dataReader = cmd.ExecuteReader();
            while (dataReader.Read())
            {
                Dictionary<string, string> voterDetails = new Dictionary<string, string>();
                voterDetails.Add("LASTNAME", Decrypt(dataReader[0].ToString()));
                voterDetails.Add("FIRSTNAME", Decrypt(dataReader[1].ToString()));
                voterDetails.Add("MATERNALNAME", Decrypt(dataReader[2].ToString()));

                voterDetails.Add("SEX", dataReader[3].ToString());
                voterDetails.Add("CIVILSTATUS", dataReader[4].ToString());
                voterDetails.Add("ADDRESS", dataReader[5].ToString());
                byte[] bytes = Encoding.UTF8.GetBytes(dataReader[5].ToString());
                string utfString = Encoding.UTF8.GetString(bytes);

                voterDetails.Add("BDAY YEAR", Decrypt(dataReader[6].ToString()));
                voterDetails.Add("BDAY MONTH", Decrypt(dataReader[7].ToString()));
                voterDetails.Add("BDAY DAY", Decrypt(dataReader[8].ToString()));

                voterDetails.Add("REG DATE", dataReader[9].ToString());
                voterDetails.Add("UPDATE DATE", dataReader[10].ToString());
                voterDetails.Add("VINP1", Decrypt(dataReader[11].ToString()));
                voterDetails.Add("VINP2", Decrypt(dataReader[12].ToString()));
                voterDetails.Add("VINP3", Decrypt(dataReader[13].ToString()));

                //retList.Add(voterDetails);
                yield return voterDetails;

                //PrintDetails(voterDetails);
            }

            Console.WriteLine("--");
            dataReader.Close();
        }

        public IEnumerable<VoterInfoComelec> IterateAll2()
        {
            MySqlCommand cmd = new MySqlCommand(_queryAll, _dbClientRead);
            cmd.CommandTimeout = int.MaxValue;
            MySqlDataReader dataReader = cmd.ExecuteReader();
            while (dataReader.Read())
            {
                VoterInfoComelec voterInfo = new VoterInfoComelec();
                try
                {
                    voterInfo.Lastname = Decrypt(dataReader[0].ToString());
                    voterInfo.Firstname = Decrypt(dataReader[1].ToString());                    
                }
                catch(Exception)
                {
                    Console.WriteLine("Unable to read/decrypt entry!");
                    continue;
                }
                try
                {
                    voterInfo.Maternalname = Decrypt(dataReader[2].ToString());
                    voterInfo.Sex = dataReader[3].ToString();
                    voterInfo.CivilStatus = dataReader[4].ToString();
                    voterInfo.Address = dataReader[5].ToString();
                }
                catch(Exception)
                {
                    Console.WriteLine("Unable to set other info for {0}, {1} - {2}", voterInfo.Lastname, voterInfo.Firstname, voterInfo.Maternalname);
                }

                try
                {
                    string bdayYear = Decrypt(dataReader[6].ToString());
                    string bdayMonth = Decrypt(dataReader[7].ToString());
                    string bdayDay = Decrypt(dataReader[8].ToString());

                    voterInfo.DateOfBirth = new DateTime(
                            Convert.ToInt32(bdayYear),
                            Convert.ToInt32(bdayMonth),
                            Convert.ToInt32(bdayDay)
                        );
                }
                catch(Exception ex)
                {
                    Console.WriteLine("Unable to set birthday for {0}, {1} - {2}", voterInfo.Lastname, voterInfo.Firstname, voterInfo.Maternalname);
                }


                yield return voterInfo;
            }

            dataReader.Close();
        }

        public List<Dictionary<string, string>> SearchData2(string inFirstName, string inMaternalName, string inLastName)
        {
            List<Dictionary<string, string>> retList = new List<Dictionary<string, string>>();
            string qryFirstName = string.IsNullOrEmpty(inFirstName) ? "FIRSTNAME" : inFirstName;
            string qryMaternalName = string.IsNullOrEmpty(inMaternalName) ? "=MATERNALNAME_P" : string.Format("LIKE '{0}'", inMaternalName);
            string qryLastName = string.IsNullOrEmpty(inLastName) ? "=LASTNAME_P" : inLastName;
            string querySearch = string.Format(_querySearch2, qryFirstName, qryMaternalName, qryLastName);

            MySqlCommand cmd = new MySqlCommand(querySearch, _dbClientRead);
            MySqlDataReader dataReader = cmd.ExecuteReader();
            while (dataReader.Read())
            {
                Dictionary<string, string> voterDetails = new Dictionary<string, string>();
                voterDetails.Add("LASTNAME", Decrypt(dataReader[0].ToString()));
                voterDetails.Add("FIRSTNAME", Decrypt(dataReader[1].ToString()));
                voterDetails.Add("MATERNALNAME", Decrypt(dataReader[2].ToString()));

                voterDetails.Add("SEX", dataReader[3].ToString());
                voterDetails.Add("CIVILSTATUS", dataReader[4].ToString());
                voterDetails.Add("ADDRESS", dataReader[5].ToString());
                byte[] bytes = Encoding.UTF8.GetBytes(dataReader[5].ToString());
                string utfString = Encoding.UTF8.GetString(bytes);

                voterDetails.Add("BDAY YEAR", Decrypt(dataReader[6].ToString()));
                voterDetails.Add("BDAY MONTH", Decrypt(dataReader[7].ToString()));
                voterDetails.Add("BDAY DAY", Decrypt(dataReader[8].ToString()));

                voterDetails.Add("REG DATE", dataReader[9].ToString());
                voterDetails.Add("UPDATE DATE", dataReader[10].ToString());
                voterDetails.Add("VINP1", Decrypt(dataReader[11].ToString()));
                voterDetails.Add("VINP2", Decrypt(dataReader[12].ToString()));
                voterDetails.Add("VINP3", Decrypt(dataReader[13].ToString()));

                retList.Add(voterDetails);

                PrintDetails(voterDetails);
            }

            Console.WriteLine("--");
            return retList;
        }
    }
}

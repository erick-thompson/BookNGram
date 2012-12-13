using System.Security.Cryptography;
using System.Text;

namespace ETL
{
    public abstract class GramLoader
    {
        private readonly SHA1 _ShaHasher = SHA1.Create();

        protected byte[] BuildHash(string s)
        {
            return _ShaHasher.ComputeHash(Encoding.UTF8.GetBytes(s));
        }

    }
}
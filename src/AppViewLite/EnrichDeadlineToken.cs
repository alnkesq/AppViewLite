using System.Threading.Tasks;
namespace AppViewLite
{
    public record struct EnrichDeadlineToken(Task DeadlineReached)
    {
        public static EnrichDeadlineToken Create()
        {
            return new EnrichDeadlineToken(Task.Delay(1500));
        }
    }

}


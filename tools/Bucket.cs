namespace tools
{
    public sealed class Bucket
    {
        public string Name { get; set; }
        public string AwsAccessKeyId { get; set; }
        public string AwsSecretAccessKey { get; set; }
        public string AwsSessionToken { get; set; }
        public string Region { get; set; }
    }
}
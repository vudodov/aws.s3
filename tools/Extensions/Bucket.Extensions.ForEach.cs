using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace tools
{
    public static class BucketExtensionsForEach
    {
        public static async void ForEachFileAsync(this Bucket bucket, string prefix,
            Action<AmazonS3Client, S3Object> operate,
            Action<ListObjectsV2Response> onFailure,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (var s3Client = string.IsNullOrWhiteSpace(bucket.AwsAccessKeyId)
                ? new AmazonS3Client(RegionEndpoint.GetBySystemName(bucket.Region))
                : new AmazonS3Client(bucket.AwsAccessKeyId, bucket.AwsSecretAccessKey, bucket.AwsSessionToken,
                    RegionEndpoint.GetBySystemName(bucket.Region)))
            {
                ListObjectsV2Response response;
                var request = new ListObjectsV2Request
                {
                    BucketName = bucket.Name,
                    Prefix = prefix
                };
                
                do
                {
                    response = await s3Client.ListObjectsV2Async(request, cancellationToken);

                    if (response.HttpStatusCode == HttpStatusCode.OK)
                    {
                        request.ContinuationToken = response.NextContinuationToken;

                        response.S3Objects
                            .Where(o => !o.Key.EndsWith('/'))
                            .ToList()
                            .ForEach(o => operate(s3Client, o));
                    }
                    else
                    {
                        onFailure(response);
                    }
                    
                } while (response.IsTruncated && !cancellationToken.IsCancellationRequested);
            }
        }

        public static async void ForEachFileAsync(this Bucket bucket, string prefix,
            Func<AmazonS3Client, S3Object, Task> operateAsync,
            Action<ListObjectsV2Response, string> onFailure,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (var s3Client = string.IsNullOrWhiteSpace(bucket.AwsAccessKeyId)
                ? new AmazonS3Client(RegionEndpoint.GetBySystemName(bucket.Region))
                : new AmazonS3Client(bucket.AwsAccessKeyId, bucket.AwsSecretAccessKey, bucket.AwsSessionToken,
                    RegionEndpoint.GetBySystemName(bucket.Region)))
            {
                ListObjectsV2Response response = await s3Client.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = bucket.Name,
                    Prefix = prefix
                }, cancellationToken);

                Task.WaitAll(response.S3Objects
                    .Where(o => !o.Key.EndsWith('/'))
                    .ToList()
                    .Select(o => operateAsync(s3Client, o))
                    .ToArray());
            }
        }
    }
}
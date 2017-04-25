# s3-multipart-uploader

[![Build Status](https://travis-ci.org/jeremyn/s3-multipart-uploader.svg?branch=master)](https://travis-ci.org/jeremyn/s3-multipart-uploader)

A Python program using boto3 that does a multipart file upload to AWS S3 with file integrity checking. It approximates the instructions at https://aws.amazon.com/premiumsupport/knowledge-center/s3-multipart-upload-cli/ .

## Author

[Jeremy Nation](https://jeremynation.me).

## Notes

There seems to be a problem on Amazon's side with checking the hash of the combined file (see [here](https://github.com/aws/aws-cli/issues/2559)). This program should report a problem if any of the component uploads have hash failures, or if the completed file is the wrong size, so in practice the Amazon bug should not be a serious concern. However, for extra assurance it should be possible to run an appropriate hash function against the uploaded file with AWS Lambda and then check its results.

## License

MIT (see included `LICENSE` file).

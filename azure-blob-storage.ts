import {
  BlobItem,
  BlobServiceClient,
  ContainerClient,
  StorageSharedKeyCredential,
} from '@azure/storage-blob';
import fs from 'node:fs';

export class AzureBlobStorage {
  private readonly account = process.env.AZR_BLOB_STORAGE_ACCOUNT;
  private readonly host = process.env.AZR_BLOB_STORAGE_HOST;
  private readonly key = process.env.AZR_BLOB_STORAGE_SHARED_KEY;
  private readonly container_name = process.env.AZR_BLOB_STORAGE_CONTAINER;
  private container_client: ContainerClient;

  constructor() {
    if (!this.account || !this.key || !this.host || !this.container_name)
      throw new Error('Missing required Azure parameters');

    const credential = new StorageSharedKeyCredential(this.account, this.key);
    this.container_client = new BlobServiceClient(
      this.host,
      credential,
    ).getContainerClient(this.container_name);
  }

  async listBlobs(callback?: (blob: BlobItem) => void) {
    let blobs = this.container_client.listBlobsFlat();
    for await (const blob of blobs) {
      console.log('Blob name: %s', blob.name);
      if (callback) callback(blob);
    }
  }

  async downloadBlob(storage_filename: string, local_filename: string) {
    const blob_client = this.container_client.getBlobClient(storage_filename);
    const block_blob = blob_client.getBlockBlobClient();
    await block_blob.downloadToFile(local_filename, 0);
    console.log('File %s downloaded successfully', storage_filename);
  }

  async deleteBlob(storage_filename: string) {
    const blob_client = this.container_client.getBlobClient(storage_filename);
    const block_blob = blob_client.getBlockBlobClient();
    await block_blob.delete();
    console.log('File %s removed successfully', storage_filename);
  }

  async uploadBlob(local_filename: string, storage_filename: string) {
    const blob_client = this.container_client.getBlobClient(storage_filename);
    const block_blob = blob_client.getBlockBlobClient();

    const one_megabyte = 1 * 1024 * 1024;
    const opts = {
      bufferSize: one_megabyte,
      maxBuffers: 5,
    };

    const stream = fs.createReadStream(local_filename, {
      highWaterMark: one_megabyte,
    });

    await block_blob.uploadStream(stream, opts.bufferSize, opts.maxBuffers);
    console.log('File uploaded successfully');
  }
}

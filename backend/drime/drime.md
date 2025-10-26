# Drime

Drime is a cloud storage provider. This backend allows you to use Drime with rclone.

## Configuration

Here's an example of how to configure a remote called `myDrime`:

```
rclone config
```

This will guide you through an interactive setup process:

```
n) New remote
name> myDrime
Type of storage> drime
Email address for Drime account> your.email@example.com
Password for Drime account> yourpassword
```

Once configured, you can use your Drime remote like any other rclone remote.

## Limitations

### No Checksums

Drime does not provide file checksums (MD5/SHA1). This means:
- Rclone will rely on **file size** and **modification time** for sync operations
- Use `--checksum` flag will not work with this backend
- Consider using `--size-only` for faster syncs if modification times are unreliable

### Standard Features

Standard rclone commands work as expected:

- `rclone ls myDrime:` - List files
- `rclone copy /local/path myDrime:remote/path` - Upload files
- `rclone sync myDrime:remote/path /local/path` - Sync files
- `rclone mount myDrime:remote/path /mount/point` - Mount remote

### Authentication

Authentication uses email and password credentials. The access token is automatically managed by rclone.
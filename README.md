# gody-cdn
[![Go](https://github.com/OdyseeTeam/gody-cdn/actions/workflows/go.yml/badge.svg)](https://github.com/OdyseeTeam/gody-cdn/actions/workflows/go.yml)

This project serves as a CDN for Odysee transcoded media content.
It should work for any kind of objects.

## Installation

- Install mysql 8 (5.7 might work too)
- Create a database, user and password with localhost only access (hint: use `godycdn`)
- Create the table(s) as described [here](https://github.com/OdyseeTeam/gody-cdn/blob/master/store/dbbacked.go#L17) (the link might not update as the code does so just look for the schema in that file)

#### Configuring
Copy [config.example.json](https://raw.githubusercontent.com/OdyseeTeam/gody-cdn/master/config.example.json) into `config.json` next to the binary and change what need to be changed.

Create a systemd script if you want to run it automatically on startup or as a service.

```ini
[Unit]
Description="Odysee Transcoder CDN - GodyCDN"
After=network.target

[Service]
WorkingDirectory=/home/YOURUSER
ExecStart=/home/YOURUSER/gody-cdn
User=YOURUSER
Group=YOURGROUP
Restart=on-failure
KillMode=process
LimitNOFILE=infinity

[Install]
WantedBy=multi-user.target
```

## Usage

The binary doesn't currently take any parameters:
```bash
./gody-cdn
```

## Running from Source

This project requires [Go v1.17+](https://golang.org/doc/install).

On Ubuntu you can install it with `sudo snap install go --classic`

```
git clone git@github.com:OdyseeTeam/gody-cdn.git
cd gody-cdn
make
./bin/gody-cdn
```

## Contributing

Feel free to open pull requests and issues. We can't give any guarantees your changes or requests will be met, but we'll check them all out.

## License

This project is MIT licensed.

## Security

We take security seriously. Please contact security@odysee.com regarding any security issues.

## Contact
The primary contact for this project is [@Nikooo777](https://github.com/Nikooo777) (niko-at-odysee.com)
## Build instrucitons


### OSX

```shell
export PKG_CONFIG="$(brew --prefix)/bin/pkg-config"
export PKG_CONFIG_PATH="$(brew --prefix)/lib/pkgconfig:$(brew --prefix)/share/pkgconfig:$(brew --prefix)/opt/openssl@3/lib/pkgconfig"
```

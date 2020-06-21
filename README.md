COMPRESS
========

### Graph demo

```hocon
to-compress {
    seq = 2
    url = "spirit://actors/fbp/compress/zstd?action=compress&algo=zstd"
}

to-decompress {
    seq = 3
    url = "spirit://actors/fbp/compress/zstd?action=decompress&algo=zstd"
}
```
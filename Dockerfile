# syntax=docker/dockerfile:1

# Lucky Star Notify（Go）的映像。
#
# # 為什麼是 multi-stage
#
# 第一階段有完整的 Go 工具鏈（幾百 MB），第二階段只拿走那一個編譯好的
# 執行檔。最終映像裡**沒有編譯器、沒有原始碼、沒有套件快取**——這既是
# 體積的來源，也是攻擊面的來源。對比 Java 版：那邊的 runtime 至少要一個
# JRE（`eclipse-temurin:21-jre-alpine`），因為 jar 不能自己跑。
# Go 的執行檔可以，所以連 base image 都幾乎不需要。
#
# # 為什麼一個 Dockerfile 蓋兩個程式
#
# `cmd/notify`（正式服務）與 `cmd/demo`（compose 展示用的假事件產生器）
# 建置步驟一模一樣，只差要編哪個套件。用 ARG 帶進來就好，複製一份
# Dockerfile 只會讓兩邊之後各自腐爛。
# 預設值是 notify，所以 `docker build .` 蓋出來的就是正式服務的映像——
# CI 的映像大小上限斷言檢查的就是這一個。

# ── 建置階段 ────────────────────────────────────────────────────────────
# 版本釘死到 patch：base image 換版是「會改變產出」的事件，應該是一次
# 明確的 commit，而不是某天 CI 自己浮動。
FROM golang:1.26.5-alpine AS build

WORKDIR /src

# 執行階段是 scratch（完全空的），CA 憑證得從這裡搬過去。
# golang:alpine 通常已經帶了 ca-certificates-bundle，但那是**相依套件帶進來
# 的副作用**，不是保證——base image 換版就可能消失，而症狀是建置階段
# COPY 失敗（還算好查）。明確裝一次，把隱性依賴變成顯性的。
RUN apk add --no-cache ca-certificates

# go.mod / go.sum 單獨先進來，是為了讓「下載依賴」變成獨立的一層。
# 只要依賴沒變，改任何 .go 檔都能直接命中快取，不必重新下載。
# 順序寫反（先 COPY . .）的話，每改一行程式碼就要重抓一次全部依賴。
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# 要編哪個 cmd。宣告在這裡（而不是檔案最上方）是因為 ARG 的作用域只到
# 當前 stage；跨 stage 要用就得在每個 stage 各自重新宣告。
ARG CMD=notify

# BuildKit 會自動帶入 TARGETOS / TARGETARCH，所以 `docker buildx build
# --platform linux/arm64` 這種跨平台建置不必改 Dockerfile。
ARG TARGETOS=linux
ARG TARGETARCH=amd64

# 三個編譯選項各有理由：
#
#	CGO_ENABLED=0   完全靜態連結。沒有它，執行檔會動態依賴宿主的 libc，
#	                distroless static 裡沒有 libc，容器會直接 exec 失敗。
#	                本專案的依賴全是純 Go（kafka-go 選它就是為了這件事），
#	                所以關掉 cgo 不會少任何功能。
#	-trimpath       把 /src/... 這種建置機器上的絕對路徑從執行檔裡拿掉。
#	                好處有二：panic 堆疊不會洩漏建置環境的目錄結構，
#	                以及同一份原始碼在不同機器上編出位元組相同的結果。
#	-ldflags="-s -w" 拿掉符號表與 DWARF 除錯資訊，執行檔大約少 25~30%。
#	                代價是 gdb 追不了、panic 堆疊少了行號——這是**刻意的
#	                取捨**：正式環境靠結構化日誌與 Prometheus 指標定位問題，
#	                不靠進容器開除錯器。要除錯就在本機用未剝離的版本重現。
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# ── 執行階段 ────────────────────────────────────────────────────────────
# scratch＝完全空的映像，連一個 byte 都沒有。
#
# # 為什麼不是 distroless
#
# 計畫書 §4.6 原本寫 gcr.io/distroless/static-debian12。但它多帶了不少我們用不到的東西
# （DoD 要求映像 < 20 MB，CI 有斷言）：tzdata、base-files、media-types…
# 本服務不做任何時區換算（時間戳原樣搬運，容器一律 UTC），付那份體積沒有回報。
#
# 改成 scratch 之後要**自己補回**三樣 distroless 免費提供的東西，
# 少任何一樣都會出現難查的症狀：
#
#	CA 憑證      少了它，未來若接 TLS 的 Kafka 會拿到
#	             "x509: certificate signed by unknown authority"，
#	             而那個訊息完全指不到「映像裡沒有憑證」這個真因。
#	             現在用不到，但 270 KB 的保險值得買。
#	非 root 使用者  distroless 用 `USER nonroot` 是靠映像裡的 /etc/passwd；
#	             scratch 沒有那個檔，所以只能寫**數字** UID（見下面）。
#	時區資料      刻意不補。真的需要具名時區時，Go 有 `import _ "time/tzdata"`
#	             可以把資料編進執行檔（約 +450 KB），不必回頭改 base image。
#
# 沒有 shell 的兩個實際後果，先寫在這裡免得之後撞牆：
#  1. `docker exec -it <c> sh` 進不去。要看內部狀態只能靠日誌與 /metrics。
#  2. `HEALTHCHECK CMD curl ...` 用不了——所以健康檢查走執行檔自己的
#     `-healthcheck` 旗標（見 cmd/notify/main.go 的 runHealthcheck）。
FROM scratch

# 憑證從建置階段搬過來。alpine 的 ca-certificates 套件裝在這個路徑，
# 而 Go 的 crypto/x509 在 Linux 上正好會找這裡，所以不必設任何環境變數。
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=build /out/app /app

# EXPOSE 純粹是文件用途（不會真的開埠），但它讓 `docker inspect` 看得到
# 這個映像預期監聽哪裡。實際埠仍由 PORT 環境變數決定。
EXPOSE 8088

# 一定要寫**數字**，不能寫 `USER nonroot`——那需要映像裡有 /etc/passwd
# 才查得到名字，scratch 沒有，容器會直接起不來。
# 65532 是 distroless 的 nonroot UID，沿用它是為了讓兩種 base 之間切換時
# 掛載出來的檔案擁有者不會變。
#
# 用 root 跑一個只需要 bind 8088 的服務沒有任何好處；
# 而且只有 1024 以下的埠才需要特權，8088 不是。
USER 65532:65532

# exec 形式（JSON 陣列）不是風格偏好：shell 形式會變成 `/bin/sh -c ...`，
# 而這個映像根本沒有 sh。而且 exec 形式讓服務成為 PID 1 並**直接收到
# SIGTERM**——優雅關閉才會被觸發（見 cmd/notify/main.go 的 NotifyContext）。
ENTRYPOINT ["/app"]

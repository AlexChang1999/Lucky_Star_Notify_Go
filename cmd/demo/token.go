package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// mintToken 簽一張玩家 token，形狀對齊 member-service 發的 access token。
//
// # ⚠️ 這個函式為什麼不能出現在 internal/ 或 cmd/notify
//
// 推播服務只**驗**章、不**簽**章：它拿到的 secret 是用來確認「這張 token
// 是 member-service 發的」。如果同一個服務也能簽 token，那個 secret 的
// 意義就從「驗證憑據」變成「憑空製造身分的能力」，而且是在一個對外開埠、
// 沒有帳號系統的服務裡。**簽章能力只存在於這個 demo 程式**，
// 它是 compose 裡的開發用容器，正式映像（cmd/notify）完全沒有這段程式碼。
//
// # 為什麼 subject 是十進位字串
//
// hub 的私人推播索引用的 key 是 JWT 的 subject，而 consumer 那邊是把
// targetPlayerId（Java 的 Long）用 strconv.FormatInt 轉成十進位字串。
// 兩邊要對得起來，這裡就得用同一種表示法——寫成 "0x3e9" 或補零的
// "0001001" 都會讓推播靜默地送不到人，而且沒有任何錯誤訊息。
func mintToken(secret []byte, playerID int64, ttl time.Duration, now time.Time) (string, error) {
	claims := jwt.RegisteredClaims{
		Subject:   strconv.FormatInt(playerID, 10),
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
	}

	// HS256 是寫死的，不從外部帶入。理由與 auth.NewVerifier 帶
	// WithValidMethods 是同一個：演算法可設定就是演算法混淆攻擊的入口，
	// 而這裡根本沒有「支援多種演算法」的需求。
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
	if err != nil {
		return "", fmt.Errorf("簽發 token 失敗：%w", err)
	}
	return token, nil
}

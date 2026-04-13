# 国会会議録検索 API リファレンス

## エンドポイント

`https://kokkai.ndl.go.jp/api/speech`

レスポンス形式: JSON（デフォルト）または XML

## 主要パラメータ

| パラメータ | 型 | 説明 | 本調査での使い方 |
|-----------|-----|------|----------------|
| `nameOfHouse` | string | 院名 | `衆議院` |
| `nameOfMeeting` | string | 会議名 | `厚生労働委員会` 等 |
| `sessionFrom` / `sessionTo` | int | 国会回次 | 216〜220 |
| `speakerPosition` | string | 発言者肩書（部分一致） | `大臣`, `副大臣`, `大臣政務官` |
| `speaker` | string | 発言者名 | 個別指定時 |
| `any` | string | 全文検索 | テーマ絞り込み時 |
| `from` / `until` | string | 日付範囲 (YYYY-MM-DD) | 期間指定時 |
| `startRecord` | int | 開始レコード番号 | ページネーション |
| `maximumRecords` | int | 最大取得件数（上限100） | `100` |
| `recordPacking` | string | `json` or `xml` | `json` |

## レスポンス構造

```json
{
  "numberOfRecords": 1234,
  "numberOfReturn": 100,
  "startRecord": 1,
  "nextRecordPosition": 101,
  "speechRecord": [
    {
      "speechID": "...",
      "issueID": "...",
      "imageKind": "会議録",
      "searchObject": 1,
      "session": 215,
      "nameOfHouse": "衆議院",
      "nameOfMeeting": "厚生労働委員会",
      "issue": "第5号",
      "date": "2024-03-15",
      "closing": null,
      "speechOrder": 5,
      "speaker": "山田太郎",
      "speakerYomi": "やまだたろう",
      "speakerGroup": "自由民主党",
      "speakerPosition": "厚生労働副大臣",
      "speakerRole": "政府参考人",
      "speech": "○山田副大臣　ただいまの御質問に...",
      "startPage": 10,
      "speechURL": "https://kokkai.ndl.go.jp/...",
      "meetingURL": "https://kokkai.ndl.go.jp/...",
      "pdfURL": "https://kokkai.ndl.go.jp/..."
    }
  ]
}
```

## 答弁者分類ロジック

### speakerPosition による役職判定

- `国務大臣` or `○○大臣` → 大臣
- `副大臣` → 副大臣
- `大臣政務官` → 政務官
- `政府参考人` `参考人` `委員長` `事務局` → 対象外

### 担当/非担当の判定

`speakerPosition` フィールドから省庁名を抽出し、委員会の所管省庁と照合する。

**委員会→所管省庁マッピング（主要なもの）**:

| 委員会 | 主要所管省庁 |
|--------|------------|
| 厚生労働委員会 | 厚生労働省 |
| 経済産業委員会 | 経済産業省 |
| 総務委員会 | 総務省 |
| 法務委員会 | 法務省 |
| 外務委員会 | 外務省 |
| 財務金融委員会 | 財務省、金融庁 |
| 文部科学委員会 | 文部科学省 |
| 農林水産委員会 | 農林水産省 |
| 国土交通委員会 | 国土交通省 |
| 環境委員会 | 環境省 |
| 内閣委員会 | 内閣府、内閣官房、デジタル庁 等 |
| 予算委員会 | 全省庁（特殊：担当/非担当の区別が意味をなさない）|
| 決算行政監視委員会 | 全省庁（同上）|

**注意**: 予算委員会・決算行政監視委員会は全省庁が対象なので、本調査からは除外する。

### speakerPosition の値の例

実際のデータに含まれる値（部分一致検索を活用）:

- `厚生労働大臣` / `厚生労働副大臣` / `厚生労働大臣政務官`
- `内閣総理大臣`
- `経済産業副大臣`
- `総務大臣政務官`
- `国務大臣（○○担当）`

## ページネーション

`maximumRecords` の上限は100。全件取得するには:

```python
start = 1
while start <= total_records:
    params["startRecord"] = start
    # fetch...
    start = response["nextRecordPosition"]
    time.sleep(1)  # レート制限遵守
```

## 既知の注意点

- `speakerPosition` は部分一致検索。`大臣` で検索すると `副大臣` `大臣政務官` もヒットする → 個別に取得するか、取得後にフィルタ
- `speakerGroup` は政党名が入るが、大臣等の場合は空になることがある
- 発言テキスト (`speech`) には冒頭に `○山田副大臣　` のような呼称が含まれる → パース時に除去
- 同一会議で複数回発言する場合、各発言が別レコードになる

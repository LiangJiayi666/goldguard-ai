from __future__ import annotations
"""关键词匹配工具：页面文字 / OCR 文字落库时自动搜索的关键词。"""


import re
import json
from pathlib import Path
from typing import Any

PLATFORM_KEYWORDS = {
    "xhs": ("xhs", "xiaohongshu", "小红书"),
    "douyin": ("douyin", "抖音", "dy"),
    "kuaishou": ("kuaishou", "快手", "ks"),
}

# 会触发“账号/品牌搜索词提炼”的载体线索。这个集合有意独立于风险词集合：
# 一段文字即使没有点名抖音/小红书/快手，只要写了“小程序：某品牌”、
# “微信搜索某名称”等，也应进入下一轮 LLM，供跨平台反查关联账号。
DISCOVERY_CUE_KEYWORDS = [
    "xhs", "小红书", "xiaohongshu",
    "douyin", "dy", "抖音",
    "快手", "ks", "kuaishou",
    "微信", "wx", "wechat", "weixin",
    "微博", "weibo",
    "公众号", "服务号", "订阅号", "视频号", "小程序", "快应用", "app",
    "账号", "官方号", "用户名", "昵称",
    "搜索", "搜一搜", "扫码", "二维码",
    "官网", "商城", "旗舰店",
]

PAGE_KEYWORDS = [
    # 社媒与即时通讯载体
    "xhs", "小红书", "xiaohongshu",
    "微信", "wx", "wechat", "weixin",
    "douyin", "dy", "抖音",
    "快手", "ks", "kuaishou",
    "微博", "weibo",
    "公众号", "服务号", "订阅号", "视频号",
    "小程序", "app", "扫码", "二维码",
    # 经营方式与交易流程
    "账号", "业务", "流程", "步骤", "交易",
    "购买", "选购", "下单", "订购", "预约", "预订", "购物车",
    "商城", "旗舰店", "官网",
    "批发", "零售", "定制", "加盟", "招商", "代理",
    "金价", "报价", "定价", "工费", "一口价", "按克",
    "回收", "兑换", "提货", "自提", "配送", "发货",
    "在线支付", "客服", "热线",
    # 涉金风险业务关键词
    "黄金期货",
    "黄金TD交易", "黄金T+D交易",
    "黄金预定价", "预订价",
    "上海黄金交易所会员", "上海黄金交易所", "上金所",
    "黄金买涨买跌", "买涨买跌",
    "黄金锁价延期交易", "锁价", "提前锁价", "延期交易",
    "高收益", "稳赚不赔", "保本",
    "保证金", "补仓", "追保",
    "售后返租", "售后回租",
    "黄金交易平台",
    "售后保管", "代管",
    "寄售",
    "溢价回购",
    "黄金保管",
    "黄金委托租赁", "委托租赁",
    "共享黄金",
    "黄金预售",
    "积分返利",
    "低价出售黄金",
    "黄金托管",
    "黄金寄存",
    "黄金理财", "委托理财",
    "黄金回购",
    "存金生息",
    "黄金质押",
]

# 社媒账号作品/帖子正文使用的关键词集；当前与页面集保持一致，
# 后续可针对帖子场景独立扩展。
SOCIAL_POST_KEYWORDS = PAGE_KEYWORDS.copy()

# 保持旧版兼容
KEYWORDS = PAGE_KEYWORDS


def match_keywords(text: str | None, scope: str = "pages") -> list[str]:
    """返回文本命中的关键词（按关键词顺序，不区分大小写）。

    scope:
      - "pages": 表二/表三（网页文字、图片 OCR 文字）
      - "social_posts": 表六（社媒账号作品/帖子标题+正文）

    纯ASCII词要求两侧不是字母/数字，避免 dy/ks/wx 误伤英文单词。
    """
    if not text:
        return []
    keywords = SOCIAL_POST_KEYWORDS if scope == "social_posts" else PAGE_KEYWORDS
    lowered = text.lower()
    hits: list[str] = []
    for keyword in keywords:
        if keyword.isascii():
            if re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", lowered):
                hits.append(keyword)
        elif keyword in text:
            hits.append(keyword)
    return hits


def match_platforms(text: str | None) -> list[str]:
    """Return social platforms mentioned in one page/OCR/post text."""
    if not text:
        return []
    lowered = text.lower()
    platforms: list[str] = []
    for platform, keywords in PLATFORM_KEYWORDS.items():
        for keyword in keywords:
            if keyword.isascii():
                found = re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", lowered)
            else:
                found = keyword in text
            if found:
                platforms.append(platform)
                break
    return platforms


def match_discovery_cues(text: str | None) -> list[str]:
    """返回能提示账号、品牌或网络载体名称的词。"""
    if not text:
        return []
    lowered = text.lower()
    hits: list[str] = []
    for keyword in DISCOVERY_CUE_KEYWORDS:
        if keyword.isascii():
            found = re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", lowered)
        else:
            found = keyword in text
        if found:
            hits.append(keyword)
    return hits


def keyword_records(texts: list[dict[str, Any] | str], source: str) -> list[dict[str, Any]]:
    """Annotate every input text, including entries with no keyword hits."""
    records: list[dict[str, Any]] = []
    for item in texts:
        text = item if isinstance(item, str) else str(item.get("text") or item.get("body") or "")
        record = {
            "source": source,
            "text": text,
            "keywords": match_keywords(text),
            "platforms": match_platforms(text),
            "discovery_cues": match_discovery_cues(text),
        }
        # Preserve provenance for the enterprise-level risk review.
        if isinstance(item, dict):
            for key in ("source_url", "artifact_path", "platform", "content_type", "title"):
                if item.get(key):
                    record[key] = item[key]
        records.append(record)
    return records


def write_keyword_records(texts: list[dict[str, Any] | str], source: str, output_path: Path) -> list[dict[str, Any]]:
    records = keyword_records(texts, source)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return records

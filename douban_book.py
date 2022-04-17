import json

import requests

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36 Edg/97.0.1072.55",
    "Accept": "application/json", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "sec-ch-ua-platform": "Windows", "X-Requested-With": "XMLHttpRequest",
    "Cookie": 'viewed="5387403"; bid=xqA4UUMHBjk; gr_user_id=e24f6d96-ff2a-45d5-ad07-cddf179b2fde; ll="118275"',
    "Origin": "https://read.douban.com",
    "Referer": "https://read.douban.com/category/1?sort=hot&sale=&page=2",
    "Sec-Fetch-Mode": "cors"}


def obtainResource(url: str):
    body = {"sort":"hot","page":2,"kind":1,"query":"\n    query getFilterWorksList($works_ids: [ID!]) {\n      worksList(worksIds: $works_ids) {\n        \n    \n    title\n    cover\n    url\n    isBundle\n    coverLabel(preferVip: true)\n  \n    \n  url\n  title\n\n    \n  author {\n    name\n    url\n  }\n  origAuthor {\n    name\n    url\n  }\n  translator {\n    name\n    url\n  }\n\n    \n  abstract\n  authorHighlight\n  editorHighlight\n\n    \n    isOrigin\n    kinds {\n      \n    name @skip(if: true)\n    shortName @include(if: true)\n    id\n  \n    }\n    ... on WorksBase @include(if: true) {\n      wordCount\n      wordCountUnit\n    }\n    ... on WorksBase @include(if: false) {\n      inLibraryCount\n    }\n    ... on WorksBase @include(if: false) {\n      \n    isEssay\n    \n    ... on EssayWorks {\n      favorCount\n    }\n  \n    \n    \n    averageRating\n    ratingCount\n    url\n    isColumn\n    isFinished\n  \n  \n  \n    }\n    ... on WorksBase @include(if: false) {\n      isColumn\n      isEssay\n      onSaleTime\n      ... on ColumnWorks {\n        updateTime\n      }\n    }\n    ... on WorksBase @include(if: true) {\n      isColumn\n      ... on ColumnWorks {\n        isFinished\n      }\n    }\n    ... on EssayWorks {\n      essayActivityData {\n        \n    title\n    uri\n    tag {\n      name\n      color\n      background\n      icon2x\n      icon3x\n      iconSize {\n        height\n      }\n      iconPosition {\n        x y\n      }\n    }\n  \n      }\n    }\n    highlightTags {\n      name\n    }\n  \n    \n  ... on WorksBase {\n    copyrightInfo {\n      newlyAdapted\n      newlyPublished\n      adaptedName\n      publishedName\n    }\n  }\n\n    isInLibrary\n    ... on WorksBase @include(if: false) {\n      \n    fixedPrice\n    salesPrice\n    isRebate\n  \n    }\n    ... on EbookWorks {\n      \n    fixedPrice\n    salesPrice\n    isRebate\n  \n    }\n    ... on WorksBase @include(if: true) {\n      ... on EbookWorks {\n        id\n        isPurchased\n        isInWishlist\n      }\n    }\n  \n        id\n        isOrigin\n      }\n    }\n  ","variables":{}}
    print(json.dumps(body))
    r = requests.post("https://read.douban.com/j/kind/", headers=headers, data=json.dumps(body))

    if r.status_code == 200:
        print(r.text)
    else:
        print(r.status_code)


if __name__ == '__main__':
    obtainResource("")

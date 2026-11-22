# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""MkDocs hook: simplify the page table of contents.

Shows root-level (h2) sections only, strips leading section numbers such as
"3.", "2.1" or "12a." so entries read consistently across docs, and drops the
in-page "Contents" section, which duplicates the TOC.
"""
import re

_NUMBER_PREFIX = re.compile(r"^\s*\d+[a-z]?(?:\.\d+)*\.?\s+")


def _simplify(items):
    kept = []
    for item in items:
        if item.title.strip().lower() == "contents":
            continue
        item.title = _NUMBER_PREFIX.sub("", item.title)
        item.children = []
        kept.append(item)
    return kept


def on_page_context(context, page, config, nav):
    if page.toc:
        # The h1 page title is the single top-level item; its children are the h2s.
        for top in page.toc.items:
            top.children = _simplify(top.children)
    return context

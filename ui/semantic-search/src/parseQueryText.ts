/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { parser } from "./parser/index.js";
import invariant from "tiny-invariant";

export type ParsedQuery = {
  searchText: string;
  filters: { keyword: string; value: string }[];
};
type InternalParsedQuery = { keyword: string; value: string }[];

export const parseQueryText = (queryText: string): ParsedQuery => {
  const tree = parser.parse(queryText);
  const parsedQuery: InternalParsedQuery = [];
  let currentString: string | undefined;
  let currentFilter: { keyword: string; value: string } | undefined;
  tree.iterate({
    enter: (node) => {
      switch (node.name) {
        case "QuotedString":
          currentString = queryText.slice(node.from + 1, node.to - 1);
          break;
        case "Word":
          currentString = queryText.slice(node.from, node.to);
          break;
        case "Filter":
          currentFilter = {} as any;
          break;
      }
    },
    leave: (node) => {
      switch (node.name) {
        case "SearchText": {
          invariant(
            currentString,
            "Expected currentString to be defined when leaving `SearchText` node"
          );
          parsedQuery.push({ keyword: "searchText", value: currentString });
          currentString = undefined;
          break;
        }
        case "Filter": {
          invariant(
            currentFilter,
            "Expected a filter object to be defined when leaving `Filter` node"
          );
          parsedQuery.push(currentFilter);
          currentFilter = undefined;
          break;
        }
        case "FilterKeyword": {
          invariant(
            currentFilter,
            "Expected a filter object to be defined when leaving `FilterKeyword` node"
          );
          invariant(
            currentString,
            "Expected currentString to be defined when leaving `FilterKeyword` node"
          );
          currentFilter.keyword = currentString;
          currentString = undefined;
          break;
        }
        case "FilterValue": {
          invariant(
            currentFilter,
            "Expected a filter object to be defined when leaving `FilterValue` node"
          );
          invariant(
            currentString,
            "Expected currentString to be defined when leaving `FilterValue` node"
          );
          currentFilter.value = currentString;
          currentString = undefined;
          break;
        }
      }
    },
  });
  const searchTextFilters = parsedQuery.filter(
    (filter) => filter.keyword === "searchText"
  );
  const otherFilters = parsedQuery.filter(
    (filter) => filter.keyword !== "searchText"
  );
  return {
    searchText: searchTextFilters.map((filter) => filter.value).join(" "),
    filters: otherFilters,
  } as const satisfies ParsedQuery;
};

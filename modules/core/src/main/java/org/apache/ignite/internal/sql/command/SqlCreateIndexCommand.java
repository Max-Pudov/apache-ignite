/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import static org.apache.ignite.internal.sql.SqlKeyword.ASC;
import static org.apache.ignite.internal.sql.SqlKeyword.DESC;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.INLINE_SIZE;
import static org.apache.ignite.internal.sql.SqlKeyword.ON;
import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIfNotExists;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * CREATE INDEX command.
 */
public class SqlCreateIndexCommand implements SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Index name. */
    private String idxName;

    /** IF NOT EXISTS flag. */
    private boolean ifNotExists;

    /** Spatial index flag. */
    private boolean spatial;

    /** Columns. */
    @GridToStringInclude
    private Collection<SqlIndexColumn> cols;

    /** Column names. */
    @GridToStringExclude
    private Set<String> colNames;

    /** Inline size. Zero effectively disables inlining. */
    private int inlineSize = QueryIndex.DFLT_INLINE_SIZE;

    /** NOLOGGING flag */
    private boolean noLogging;

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @return IF NOT EXISTS flag.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * Indicates if NOLOGGING flag was specified. This flag disables WAL during index creation.
     *
     * @return true if the command has NOLOGGING option, false otherwise.
     */
    public boolean noLogging() {
        return noLogging;
    }

    /**
     * @return Spatial index flag.
     */
    public boolean spatial() {
        return spatial;
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @param spatial Spatial index flag.
     * @return This instance.
     */
    public SqlCreateIndexCommand spatial(boolean spatial) {
        this.spatial = spatial;

        return this;
    }

    /**
     * @return Columns.
     */
    public Collection<SqlIndexColumn> columns() {
        return cols != null ? cols : Collections.<SqlIndexColumn>emptySet();
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        ifNotExists = parseIfNotExists(lex);

        idxName = parseIndexName(lex);

        skipIfMatchesKeyword(lex, ON);

        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseColumnList(lex);

        parseInlineSize(lex);

        parseNoLogging(lex);

        return this;
    }

    /**
     * Pasrse index name.
     *
     * @param lex Lexer.
     * @return Index name.
     */
    private static @Nullable String parseIndexName(SqlLexer lex) {
        if (matchesKeyword(lex.lookAhead(), ON))
            return null;

        return parseIdentifier(lex, IF);
    }

    /**
     * Parses inline size option if exists.
     *
     * @param lex Lexer.
     */
    private void parseInlineSize(SqlLexer lex) {
        SqlLexerToken nextTok = lex.lookAhead();

        if (matchesKeyword(nextTok, INLINE_SIZE)) {
            lex.shift();

            int stmtInlineSize = SqlParserUtils.parseInt(lex);

            if (stmtInlineSize < 0)
                throw error(lex, "Inline size should be positive: " + stmtInlineSize);

            inlineSize = stmtInlineSize;
        }
    }

    /**
     * Parses NOLOGGING flag if exists and sets appropriate flag.
     *
     * @param lex The Lexer.
     */
    private void parseNoLogging(SqlLexer lex) {
        if (matchesKeyword(lex.lookAhead(), INLINE_SIZE)) {
            lex.shift();

            noLogging = true;
        }
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumnList(SqlLexer lex) {
        if (!lex.shift() || lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        while (true) {
            parseIndexColumn(lex);

            if (skipCommaOrRightParenthesis(lex))
                break;
        }
    }

    /**
     * @param lex Lexer.
     */
    private void parseIndexColumn(SqlLexer lex) {
        String name = parseIdentifier(lex);
        boolean desc = false;

        SqlLexerToken nextToken = lex.lookAhead();

        if (matchesKeyword(nextToken, ASC) || matchesKeyword(nextToken, DESC)) {
            lex.shift();

            if (matchesKeyword(lex, DESC))
                desc = true;
        }

        addColumn(lex, new SqlIndexColumn(name, desc));
    }

    /**
     * @param lex Lexer.
     * @param col Column.
     */
    private void addColumn(SqlLexer lex, SqlIndexColumn col) {
        if (cols == null) {
            cols = new LinkedList<>();
            colNames = new HashSet<>();
        }

        if (!colNames.add(col.name()))
            throw error(lex, "Column already defined: " + col.name());

        cols.add(col);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateIndexCommand.class, this);
    }
}

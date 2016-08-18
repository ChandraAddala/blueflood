package com.datastax.driver.core;

import java.nio.ByteBuffer;

public class DatastaxM3PToken extends Token {

    final Token token;

    public DatastaxM3PToken(org.apache.cassandra.dht.LongToken longToken) {
        this(longToken.token);
    }

    public DatastaxM3PToken(Long tokenValue) {
        this.token = M3PToken.FACTORY.fromString(tokenValue.toString());
    }

    @Override
    public DataType getType() {
        return token.getType();
    }

    @Override
    public Object getValue() {
        return token.getValue();
    }

    @Override
    public ByteBuffer serialize(ProtocolVersion protocolVersion) {
        return token.serialize(protocolVersion);
    }

    @Override
    public int compareTo(Token o) {
        return token.compareTo(o);
    }
}

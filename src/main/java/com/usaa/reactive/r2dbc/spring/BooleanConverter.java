package com.usaa.reactive.r2dbc.spring;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

@ReadingConverter
/* package private */ class BooleanConverter implements Converter<Short, Boolean> {
    @Override
    public Boolean convert(Short source) {
        return source != 0;
    }
}

package com.usaa.reactive.r2dbc.spring.integration.tictactoe;

import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;

import java.util.Optional;

/* package private */ interface SearchRepository {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    Flux<GameStats> search(Optional<String>          player,
                           Optional<String>          winningPlayer,
                           Optional<GameStats.Space> winningMove,
                           Pageable                  pageable);
}

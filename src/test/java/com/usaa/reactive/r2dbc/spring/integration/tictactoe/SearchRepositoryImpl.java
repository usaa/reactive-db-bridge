package com.usaa.reactive.r2dbc.spring.integration.tictactoe;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Criteria;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.springframework.data.relational.core.query.Criteria.where;
import static org.springframework.data.relational.core.query.Query.query;

@RequiredArgsConstructor
/* package private */ class SearchRepositoryImpl implements SearchRepository {
    private final R2dbcEntityTemplate entityTemplate;

    @Override
    public
    Flux<GameStats> search(Optional<String>          player,
                           Optional<String>          winningPlayer,
                           Optional<GameStats.Space> winningMove,
                           Pageable                  pageable) {

        List<Criteria> criteriaList = new ArrayList<>();
        player       .ifPresent(s -> criteriaList.add(where("playerX").is(s).or("playerY").is(s)));
        winningPlayer.ifPresent(s -> criteriaList.add(where("winner").is(s).or("playerY").is(s)));
        winningMove  .ifPresent(s -> criteriaList.add(where("lastMove").is(s)));

        Criteria criteria = Criteria.empty().and(criteriaList);


        return entityTemplate.select(GameStats.class)
                .matching(query(criteria).with(pageable))
                .all();
    }
}

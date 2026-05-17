package com.pos.service;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

import com.pos.entity.Offer;

@Data
public class RuleInput {

    private Double amount;
    private int creditScore;
    private String merchantCategory;
    private int rewardPoints;

    private List<Offer> offers = new ArrayList<>();

    public void addOffer(Offer offer) {
        offers.add(offer);
    }
}

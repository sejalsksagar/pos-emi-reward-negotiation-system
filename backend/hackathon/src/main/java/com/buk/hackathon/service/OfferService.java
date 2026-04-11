package com.buk.hackathon.service;


import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import com.buk.hackathon.entity.Offer;

import java.util.*;

@Service
@RequiredArgsConstructor
public class OfferService {

    private final DatasetService datasetService;

    public List<Offer> generateOffers(String userId, Double amount) {

        Map<String, String> data = datasetService.getUserData(userId);

        int creditScore = Integer.parseInt(data.getOrDefault("creditScore", "600"));
        String category = data.getOrDefault("merchantCategory", "general");
        int rewardPoints = Integer.parseInt(data.getOrDefault("rewardPoints", "0"));

        List<Offer> offers = new ArrayList<>();

        // EMI Offer
        if (amount > 100000) {
            offers.add(Offer.builder()
                    .offerId(UUID.randomUUID().toString())
                    .type("EMI")
                    .description(creditScore > 700 ?
                            "No-cost EMI for 12 months" :
                            "EMI at 12% interest")
                    .build());
        }

        // Cashback Offer
        if ("electronics".equalsIgnoreCase(category)) {
            offers.add(Offer.builder()
                    .offerId(UUID.randomUUID().toString())
                    .type("CASHBACK")
                    .description("10% cashback on electronics")
                    .build());
        }

        // Reward Points
        if (rewardPoints > 1000) {
            offers.add(Offer.builder()
                    .offerId(UUID.randomUUID().toString())
                    .type("REWARD")
                    .description("Redeem 1000 reward points")
                    .build());
        }

        return offers;
    }
}

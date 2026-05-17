package com.pos.service;


import lombok.RequiredArgsConstructor;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.springframework.stereotype.Service;

import com.pos.entity.Offer;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class OfferService {

    private final DatasetService datasetService;
    private final KieContainer kieContainer;

    public List<Offer> generateOffers(String userId, Double amount) {

        Map<String, String> data = datasetService.getUserData(userId);

        int creditScore = Integer.parseInt(data.getOrDefault("creditScore", "650"));
        String category = data.getOrDefault("merchantCategory", "general").toLowerCase();
        int rewardPoints = Integer.parseInt(data.getOrDefault("rewardPoints", "0"));

        RuleInput input = new RuleInput();
        input.setAmount(amount);
        input.setCreditScore(creditScore);
        input.setMerchantCategory(category);
        input.setRewardPoints(rewardPoints);

        KieSession kieSession = null;

        try {
            kieSession = kieContainer.newKieSession("rulesSession");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create KieSession. Check kmodule.xml", e);
        }

        kieSession.insert(input);

        int fired = kieSession.fireAllRules();
        System.out.println("Rules fired: " + fired);

        kieSession.dispose();

        return input.getOffers();
    }
}
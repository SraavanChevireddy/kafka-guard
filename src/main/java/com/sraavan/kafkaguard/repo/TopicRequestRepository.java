package com.sraavan.kafkaguard.repo;

import com.sraavan.kafkaguard.domain.TopicRequestRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TopicRequestRepository extends JpaRepository<TopicRequestRecord, Long> {
    List<TopicRequestRecord> findByClusterOrderByRequestedAtDesc(String cluster);
}

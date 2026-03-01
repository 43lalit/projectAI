package com.projectai.projectai.recent;

import com.projectai.projectai.auth.AuthModels;
import com.projectai.projectai.auth.AuthService;
import com.projectai.projectai.auth.CsrfService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/recent")
public class RecentController {

    private final RecentService recentService;
    private final AuthService authService;
    private final CsrfService csrfService;

    public RecentController(RecentService recentService, AuthService authService, CsrfService csrfService) {
        this.recentService = recentService;
        this.authService = authService;
        this.csrfService = csrfService;
    }

    @GetMapping("/items")
    public ResponseEntity<List<RecentModels.RecentItemResponse>> listItems(
            @RequestParam(defaultValue = "10") int limit,
            HttpSession session
    ) {
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        return ResponseEntity.ok(recentService.listItems(user.userId(), limit));
    }

    @PostMapping("/items")
    public ResponseEntity<List<RecentModels.RecentItemResponse>> upsertItem(
            @RequestBody RecentModels.RecentItemRequest request,
            @RequestParam(defaultValue = "10") int limit,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        return ResponseEntity.ok(recentService.upsertItem(user.userId(), request, limit));
    }

    @DeleteMapping("/items")
    public ResponseEntity<Void> clearItems(
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.requireCurrentUser(session);
        recentService.clearItems(user.userId());
        return ResponseEntity.noContent().build();
    }
}

package com.projectai.projectai.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final AuthService authService;
    private final CsrfService csrfService;

    public AuthController(AuthService authService, CsrfService csrfService) {
        this.authService = authService;
        this.csrfService = csrfService;
    }

    @GetMapping("/csrf")
    public ResponseEntity<AuthModels.CsrfTokenResponse> csrf(HttpSession session) {
        return ResponseEntity.ok(new AuthModels.CsrfTokenResponse(csrfService.getOrCreateToken(session)));
    }

    @PostMapping("/register")
    public ResponseEntity<AuthModels.AuthStatusResponse> register(
            @RequestBody AuthModels.RegisterRequest request,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.register(request, session);
        return ResponseEntity.ok(new AuthModels.AuthStatusResponse(true, user));
    }

    @PostMapping("/login")
    public ResponseEntity<AuthModels.AuthStatusResponse> login(
            @RequestBody AuthModels.LoginRequest request,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        AuthModels.AuthUserResponse user = authService.login(request, session);
        return ResponseEntity.ok(new AuthModels.AuthStatusResponse(true, user));
    }

    @PostMapping("/logout")
    public ResponseEntity<AuthModels.AuthStatusResponse> logout(HttpServletRequest httpRequest, HttpSession session) {
        csrfService.requireValidToken(httpRequest, session);
        authService.logout(session);
        return ResponseEntity.ok(new AuthModels.AuthStatusResponse(false, null));
    }

    @PostMapping("/reset-password")
    public ResponseEntity<Void> resetPassword(
            @RequestBody AuthModels.ResetPasswordRequest request,
            HttpServletRequest httpRequest,
            HttpSession session
    ) {
        csrfService.requireValidToken(httpRequest, session);
        authService.resetPassword(request);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/me")
    public ResponseEntity<AuthModels.AuthStatusResponse> me(HttpSession session) {
        AuthModels.AuthUserResponse user = authService.getCurrentUser(session);
        return ResponseEntity.ok(new AuthModels.AuthStatusResponse(user != null, user));
    }
}

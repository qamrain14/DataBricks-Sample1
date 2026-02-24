import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { AuthService } from '../../core/services/auth.service';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="login-container">
      <div class="login-card">
        <h1>🏫 School Suite</h1>
        <h2>{{ isRegister() ? 'Create Account' : 'Sign In' }}</h2>
        
        @if (error()) {
          <div class="error-message">{{ error() }}</div>
        }

        <form (ngSubmit)="onSubmit()">
          @if (isRegister()) {
            <div class="form-group">
              <label for="fullName">Full Name</label>
              <input type="text" id="fullName" [(ngModel)]="fullName" name="fullName" required>
            </div>
          }
          
          <div class="form-group">
            <label for="email">Email</label>
            <input type="email" id="email" [(ngModel)]="email" name="email" required>
          </div>
          
          <div class="form-group">
            <label for="password">Password</label>
            <input type="password" id="password" [(ngModel)]="password" name="password" required>
          </div>

          <button type="submit" [disabled]="loading()">
            {{ loading() ? 'Please wait...' : (isRegister() ? 'Create Account' : 'Sign In') }}
          </button>
        </form>

        <p class="toggle-mode">
          {{ isRegister() ? 'Already have an account?' : 'Need an account?' }}
          <a href="#" (click)="toggleMode($event)">
            {{ isRegister() ? 'Sign In' : 'Register' }}
          </a>
        </p>

        <p class="demo-hint">Demo: admin&#64;school.local / Admin&#64;123!</p>
      </div>
    </div>
  `,
  styles: [`
    .login-container {
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      padding: 1rem;
    }
    .login-card {
      background: white;
      padding: 2.5rem;
      border-radius: 12px;
      box-shadow: 0 10px 40px rgba(0,0,0,0.2);
      width: 100%;
      max-width: 400px;
    }
    h1 { text-align: center; margin-bottom: 0.5rem; font-size: 1.75rem; }
    h2 { text-align: center; margin-bottom: 1.5rem; font-weight: 400; color: #666; }
    .form-group { margin-bottom: 1rem; }
    label { display: block; margin-bottom: 0.5rem; font-weight: 500; color: #333; }
    input {
      width: 100%;
      padding: 0.75rem;
      border: 1px solid #ddd;
      border-radius: 6px;
      font-size: 1rem;
      box-sizing: border-box;
    }
    input:focus { outline: none; border-color: #667eea; box-shadow: 0 0 0 3px rgba(102,126,234,0.1); }
    button {
      width: 100%;
      padding: 0.875rem;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      border: none;
      border-radius: 6px;
      font-size: 1rem;
      font-weight: 600;
      cursor: pointer;
      margin-top: 0.5rem;
    }
    button:hover:not(:disabled) { opacity: 0.9; }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    .error-message {
      background: #fee;
      color: #c00;
      padding: 0.75rem;
      border-radius: 6px;
      margin-bottom: 1rem;
      text-align: center;
    }
    .toggle-mode { text-align: center; margin-top: 1.5rem; color: #666; }
    .toggle-mode a { color: #667eea; text-decoration: none; font-weight: 500; }
    .demo-hint { text-align: center; margin-top: 1rem; font-size: 0.85rem; color: #999; }
  `]
})
export class LoginComponent {
  email = '';
  password = '';
  fullName = '';
  
  isRegister = signal(false);
  loading = signal(false);
  error = signal('');

  constructor(private authService: AuthService, private router: Router) {}

  toggleMode(event: Event): void {
    event.preventDefault();
    this.isRegister.update(v => !v);
    this.error.set('');
  }

  onSubmit(): void {
    this.loading.set(true);
    this.error.set('');

    const request = this.isRegister()
      ? this.authService.register({ email: this.email, password: this.password, fullName: this.fullName })
      : this.authService.login({ email: this.email, password: this.password });

    request.subscribe({
      next: () => {
        this.router.navigate(['/']);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(err.error?.message || 'Authentication failed. Please try again.');
      }
    });
  }
}

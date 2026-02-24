import { Routes } from '@angular/router';
import { authGuard, guestGuard } from './core/guards/auth.guard';

export const routes: Routes = [
  { path: 'login', loadComponent: () => import('./features/login/login.component').then(m => m.LoginComponent), canActivate: [guestGuard] },
  { path: '', loadComponent: () => import('./features/dashboard/dashboard.component').then(m => m.DashboardComponent), canActivate: [authGuard] },
  { path: 'curriculum', loadComponent: () => import('./features/curriculum/curriculum.component').then(m => m.CurriculumComponent), canActivate: [authGuard] },
  { path: 'commerce', loadComponent: () => import('./features/commerce/commerce.component').then(m => m.CommerceComponent), canActivate: [authGuard] },
  { path: 'bot', loadComponent: () => import('./features/bot/bot.component').then(m => m.BotComponent), canActivate: [authGuard] },
  { path: 'profile', loadComponent: () => import('./features/profile/profile.component').then(m => m.ProfileComponent), canActivate: [authGuard] },
  { path: '**', redirectTo: '' }
];
